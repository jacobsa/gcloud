// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/url"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

// A bucket that wraps another, calling its methods in a retry loop with
// randomized exponential backoff.
type retryBucket struct {
	maxSleep time.Duration
	wrapped  Bucket
}

func newRetryBucket(
	maxSleep time.Duration,
	wrapped Bucket) (b Bucket) {
	b = &retryBucket{
		maxSleep: maxSleep,
		wrapped:  wrapped,
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func shouldRetry(err error) (b bool) {
	// HTTP 50x errors.
	if typed, ok := err.(*googleapi.Error); ok {
		if typed.Code >= 500 && typed.Code < 600 {
			b = true
			return
		}
	}

	// HTTP 429 errors (GCS uses these for rate limiting).
	if typed, ok := err.(*googleapi.Error); ok {
		if typed.Code == 429 {
			b = true
			return
		}
	}

	// Network errors, which tend to show up transiently when doing lots of
	// operations in parallel. For example:
	//
	//     dial tcp 74.125.203.95:443: too many open files
	//
	if _, ok := err.(*net.OpError); ok {
		b = true
		return
	}

	// Sometimes the HTTP package helpfully encapsulates the real error in a URL
	// error.
	if urlErr, ok := err.(*url.Error); ok {
		b = shouldRetry(urlErr.Err)
		return
	}

	return
}

// Choose an appropriate delay for exponential backoff, given that we have
// already slept the given number of times for this logical request.
func chooseDelay(prevSleepCount uint) (d time.Duration) {
	const baseDelay = time.Millisecond

	// Choose a a delay in [0, 2^prevSleepCount * baseDelay).
	d = (1 << prevSleepCount) * baseDelay
	d = time.Duration(float64(d) * rand.Float64())

	return
}

// Exponential backoff for a function that might fail.
//
// This is essentially what is described in the "Best practices" section of the
// "Upload Objects" docs:
//
//     https://cloud.google.com/storage/docs/json_api/v1/how-tos/upload
//
// with the following exceptions:
//
//  *  We perform backoff for all operations.
//
//  *  The random component scales with the delay, so that the first sleep
//     cannot be as long as one second. The algorithm used matches the
//     description at http://en.wikipedia.org/wiki/Exponential_backoff.
//
//  *  We retry more types of errors; see shouldRetry above.
//
// State for total sleep time and number of previous sleeps is housed outside
// of this function to allow it to be "resumed" by multiple invocations of
// retryObjectReader.Read.
func expBackoff(
	ctx context.Context,
	desc string,
	maxSleep time.Duration,
	f func() error,
	prevSleepCount *uint,
	prevSleepDuration *time.Duration) (err error) {
	for {
		// Make an attempt. Stop if successful.
		err = f()
		if err == nil {
			return
		}

		// Do we want to retry?
		if !shouldRetry(err) {
			log.Printf(
				"Not retrying %s after error of type %T (%q): %#v",
				desc,
				err,
				err.Error(),
				err)

			return
		}

		// Choose a a delay.
		d := chooseDelay(*prevSleepCount)
		*prevSleepCount++

		// Are we out of credit?
		if *prevSleepDuration+d > maxSleep {
			// Return the most recent error.
			return
		}

		// Sleep, returning early if cancelled.
		log.Printf(
			"Retrying %s after error of type %T (%q) in %v",
			desc,
			err,
			err,
			d)

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case <-time.After(d):
			*prevSleepDuration += d
			continue
		}
	}
}

// Like expBackoff, but assumes that we've never slept before (and won't need
// to sleep again).
func oneShotExpBackoff(
	ctx context.Context,
	desc string,
	maxSleep time.Duration,
	f func() error) (err error) {
	var prevSleepCount uint
	var prevSleepDuration time.Duration

	err = expBackoff(
		ctx,
		desc,
		maxSleep,
		f,
		&prevSleepCount,
		&prevSleepDuration)

	return
}

////////////////////////////////////////////////////////////////////////
// Read support
////////////////////////////////////////////////////////////////////////

type retryObjectReader struct {
	// The context we should watch when sleeping for retries.
	ctx context.Context

	// The object name and generation we want to read.
	name       string
	generation int64

	// nil when we start or have seen a permanent error.
	wrapped io.ReadCloser

	// If we've seen an error that we shouldn't retry for, this will be non-nil
	// and should be returned permanently.
	permanentErr error

	// How many bytes we've already passed on to the user.
	bytesRead uint64

	// The number of times we've slept so far, and the total amount of time we've
	// spent sleeping.
	sleepCount    uint
	sleepDuration time.Duration
}

// Set up the wrapped reader.
func (rc *retryObjectReader) setUpWrapped() (err error)

// Set up the wrapped reader if necessary, and make one attempt to read through
// it.
//
// Clears the wrapped reader on error.
func (rc *retryObjectReader) readOnce(p []byte) (n int, err error) {
	// Set up the wrapped reader if it's not already around.
	if rc.wrapped == nil {
		err = rc.setUpWrapped()
		if err != nil {
			return
		}
	}

	// Attempt to read from it.
	n, err = rc.wrapped.Read(p)
	if err != nil {
		rc.wrapped = nil
		return
	}

	return
}

// Invariant: we never return an error from this function unless we've given up
// on retrying. In particular, we won't return a short read because the wrapped
// reader returned a short read and an error.
func (rc *retryObjectReader) Read(p []byte) (n int, err error) {
	// Whatever we do, accumulate the bytes that we're returning to the user.
	defer func() {
		if n < 0 {
			panic(fmt.Sprintf("Negative byte count: %d", n))
		}

		rc.bytesRead += uint64(n)
	}()

	// If we've already decided on a permanent error, return that.
	if rc.permanentErr != nil {
		err = rc.permanentErr
		rc.wrapped = nil
		return
	}

	// If we let an error escape below, it must be a permanent one.
	defer func() {
		if err != nil {
			rc.permanentErr = err
		}
	}()

	for {
		// Make one attempt. Make sure to accumulate the result.
		var bytesRead int
		bytesRead, err = rc.readOnce(p)
		n += bytesRead
		p = p[bytesRead:]

		// If we were successful, we're done.
		if err == nil {
			return
		}

		// Do we want to retry?
		if !shouldRetry(err) {
			log.Printf(
				"Not retrying read error of type %T (%q): %#v",
				err,
				err.Error(),
				err)

			return
		}

		// Choose a delay.
		d := chooseDelay(rc.sleepCount)
		rc.sleepCount++

		// Sleep, returning early if cancelled.
		log.Printf("Retrying after read error of type %T (%q) in %v", err, err, d)

		select {
		case <-rc.ctx.Done():
			err = rc.ctx.Err()
			return

		case <-time.After(d):
			rc.sleepDuration += d
		}
	}
}

func (rc *retryObjectReader) Close() (err error) {
	// If we don't have a wrapped reader, there is nothing useful that we can or
	// need to do here.
	if rc.wrapped == nil {
		return
	}

	// Call through.
	err = rc.wrapped.Close()

	return
}

////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////

func (rb *retryBucket) Name() (name string) {
	name = rb.wrapped.Name()
	return
}

func (rb *retryBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("NewReader(%q)", req.Name),
		rb.maxSleep,
		func() (err error) {
			rc, err = rb.wrapped.NewReader(ctx, req)
			return
		})

	return
}

func (rb *retryBucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error) {
	// We can't simply replay the request multiple times, because the first
	// attempt might exhaust some of the req.Contents reader, leaving missing
	// contents for the second attempt.
	//
	// So, copy out all contents and create a modified request that serves from
	// memory.
	contents, err := ioutil.ReadAll(req.Contents)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadAll: %v", err)
		return
	}

	reqCopy := *req
	reqCopy.Contents = bytes.NewReader(contents)

	// Call through with that request.
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("CreateObject(%q)", req.Name),
		rb.maxSleep,
		func() (err error) {
			o, err = rb.wrapped.CreateObject(ctx, &reqCopy)
			return
		})

	return
}

func (rb *retryBucket) CopyObject(
	ctx context.Context,
	req *CopyObjectRequest) (o *Object, err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("CopyObject(%q, %q)", req.SrcName, req.DstName),
		rb.maxSleep,
		func() (err error) {
			o, err = rb.wrapped.CopyObject(ctx, req)
			return
		})

	return
}

func (rb *retryBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("StatObject(%q)", req.Name),
		rb.maxSleep,
		func() (err error) {
			o, err = rb.wrapped.StatObject(ctx, req)
			return
		})

	return
}

func (rb *retryBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("ListObjects(%q)", req.Prefix),
		rb.maxSleep,
		func() (err error) {
			listing, err = rb.wrapped.ListObjects(ctx, req)
			return
		})
	return
}

func (rb *retryBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("UpdateObject(%q)", req.Name),
		rb.maxSleep,
		func() (err error) {
			o, err = rb.wrapped.UpdateObject(ctx, req)
			return
		})

	return
}

func (rb *retryBucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	err = oneShotExpBackoff(
		ctx,
		fmt.Sprintf("DeleteObject(%q)", name),
		rb.maxSleep,
		func() (err error) {
			err = rb.wrapped.DeleteObject(ctx, name)
			return
		})

	return
}
