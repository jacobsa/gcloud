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

// A program that makes random reads within a large object on GCS, reporting
// throughput and latency information.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/syncutil"
)

var fBucket = flag.String("bucket", "", "The GCS bucket from which to read.")
var fObject = flag.String("object", "", "The object within which to read.")
var fSize = flag.Uint64("size", 1<<21, "The size of each read in bytes.")
var fWorkers = flag.Int("workers", 1, "The number of workers to run.")

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

type result struct {
	BytesRead int

	// Time taken from making the HTTP request to receiving the first byte of the
	// response body.
	FirstByteLatency time.Duration

	// Time taken from making the HTTP request to receiving the last byte of the
	// response body.
	FullBodyDuration time.Duration
}

func readOnce(
	ctx context.Context,
	o *gcs.Object,
	bucket gcs.Bucket) (r result, err error) {
	// Is the object large enough?
	if o.Size < *fSize {
		err = fmt.Errorf(
			"Object of size %d not large enough for read size %d",
			o.Size,
			*fSize)

		return
	}

	// Set up an appropriate request.
	req := &gcs.ReadObjectRequest{
		Name:       o.Name,
		Generation: o.Generation,
		Range:      &gcs.ByteRange{},
	}

	req.Range.Start = uint64(rand.Int63n(int64(o.Size - *fSize)))
	req.Range.Limit = req.Range.Start + *fSize

	// Create the reader.
	start := time.Now()
	rc, err := bucket.NewReader(ctx, req)
	if err != nil {
		err = fmt.Errorf("NewReader: %v", err)
		return
	}

	defer func() {
		closeErr := rc.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("Close: %v", closeErr)
		}
	}()

	// Measure the time to first byte.
	_, err = rc.Read([]byte{0})
	if err != nil {
		err = fmt.Errorf("Read: %v", err)
		return
	}

	r.FirstByteLatency = time.Since(start)

	// And the time to read everything.
	n, err := io.Copy(ioutil.Discard, rc)
	if err != nil {
		err = fmt.Errorf("Copy: %v", err)
		return
	}

	r.FullBodyDuration = time.Since(start)
	r.BytesRead = int(n + 1)

	return
}

// Make random reads within the given object, writing results to the supplied
// channel. Stop when the stop channel is closed.
func makeReads(
	ctx context.Context,
	o *gcs.Object,
	bucket gcs.Bucket,
	results chan<- result,
	stop <-chan struct{}) (err error) {
	// Is the object large enough?
	if o.Size < *fSize {
		err = fmt.Errorf(
			"Object of size %d not large enough for read size %d",
			o.Size,
			*fSize)

		return
	}

	for {
		// Stop?
		if _, ok := <-stop; ok {
			return
		}

		// Make a read.
		var r result
		r, err = readOnce(ctx, o, bucket)
		if err != nil {
			err = fmt.Errorf("readOnce %v", err)
			return
		}

		// Write out the result.
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case results <- r:
		}
	}
}

func getBucket(ctx context.Context) (b gcs.Bucket, err error) {
	if *fBucket == "" {
		err = errors.New("You must set --bucket.")
		return
	}

	// Set up the token source.
	const scope = gcs.Scope_ReadOnly
	tokenSrc, err := google.DefaultTokenSource(context.Background(), scope)
	if err != nil {
		err = fmt.Errorf("DefaultTokenSource: %v", err)
		return
	}

	// Use that to create a GCS connection.
	cfg := &gcs.ConnConfig{
		TokenSource: tokenSrc,
	}

	conn, err := gcs.NewConn(cfg)
	if err != nil {
		err = fmt.Errorf("NewConn: %v", err)
		return
	}

	// Open the bucket.
	b, err = conn.OpenBucket(ctx, *fBucket)
	if err != nil {
		err = fmt.Errorf("OpenBucket: %v", err)
		return
	}

	return
}

func getObject(
	ctx context.Context,
	bucket gcs.Bucket) (o *gcs.Object, err error) {
	if *fObject == "" {
		err = errors.New("You must set --object.")
		return
	}

	o, err = bucket.StatObject(
		ctx,
		&gcs.StatObjectRequest{Name: *fObject})

	return
}

// Run workers until SIGINT is received. Return a slice of results.
func runWorkers(
	ctx context.Context,
	o *gcs.Object,
	bucket gcs.Bucket) (results []result, err error) {
	b := syncutil.NewBundle(ctx)

	// Set up a channel that is closed upon SIGINT.
	stop := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		<-c
		log.Printf("SIGINT received. Stopping soon...")
		close(stop)
	}()

	// Start several workers making random reads.
	var wg sync.WaitGroup
	resultChan := make(chan result)
	for i := 0; i < *fWorkers; i++ {
		wg.Add(1)
		b.Add(func(ctx context.Context) (err error) {
			defer wg.Done()
			err = makeReads(ctx, o, bucket, resultChan, stop)
			if err != nil {
				err = fmt.Errorf("makeReads: %v", err)
				return
			}

			return
		})
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Accumulate results.
	b.Add(func(ctx context.Context) (err error) {
		for r := range resultChan {
			results = append(results, r)
		}

		return
	})

	err = b.Join()
	return
}

func describeResults(results []result)

////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////

func run(ctx context.Context) (err error) {
	// Open the bucket.
	bucket, err := getBucket(ctx)
	if err != nil {
		err = fmt.Errorf("getBucket: %v", err)
		return
	}

	// Stat the object.
	o, err := getObject(ctx, bucket)
	if err != nil {
		err = fmt.Errorf("getObject: %v", err)
		return
	}

	// Make reads.
	results, err := runWorkers(ctx, o, bucket)
	if err != nil {
		err = fmt.Errorf("runWorkers: %v", err)
		return
	}

	// Print information about the results.
	describeResults(results)

	return
}

func main() {
	log.SetFlags(log.Lmicroseconds)

	err := run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
