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
	"io"
	"math/rand"
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

// Exponential backoff for a function that might fail.
//
// This is essentially what is described in the "Best practices" section of the
// "Upload Objects" docs:
//
//     https://cloud.google.com/storage/docs/json_api/v1/how-tos/upload
//
// with the following exceptions:
//
//  *  We perform backoff for all errors except:
//      *  HTTP 40x errors
//      *  Error types defined by this package
//
//  *  We perform backoff for all operations.
//
//  *  The random component scales with the delay, so that the first sleep
//     cannot be as long as one second. The algorithm used matches the
//     description at http://en.wikipedia.org/wiki/Exponential_backoff.
//
func expBackoff(
	ctx context.Context,
	maxSleep time.Duration,
	f func() error) (err error) {
	const baseDelay = time.Millisecond
	var totalSleep time.Duration

	for n := uint(0); ; n++ {
		// Make an attempt.
		err = f()

		// Is this an error we want to pass through?
		if _, ok := err.(*NotFoundError); ok {
			return
		}

		if _, ok := err.(*PreconditionError); ok {
			return
		}

		if typed, ok := err.(*googleapi.Error); ok {
			if typed.Code >= 400 && typed.Code < 500 {
				return
			}
		}

		// Choose a a delay in [0, 2^n * baseDelay).
		d := (1 << n) * baseDelay
		d = time.Duration(float64(d) * rand.Float64())

		// Are we out of credit?
		if totalSleep+d > maxSleep {
			// Return the most recent error.
			return
		}

		// Sleep, returning early if cancelled.
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case <-time.After(d):
			totalSleep += d
			continue
		}
	}
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
	err = expBackoff(
		ctx,
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
	err = expBackoff(
		ctx,
		rb.maxSleep,
		func() (err error) {
			o, err = rb.wrapped.CreateObject(ctx, req)
			return
		})

	return
}

func (rb *retryBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	err = expBackoff(
		ctx,
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
	err = expBackoff(
		ctx,
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
	err = expBackoff(
		ctx,
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
	err = expBackoff(
		ctx,
		rb.maxSleep,
		func() (err error) {
			err = rb.wrapped.DeleteObject(ctx, name)
			return
		})

	return
}
