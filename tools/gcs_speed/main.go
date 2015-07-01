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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/syncutil"
)

var fBucket = flag.String("bucket", "", "The GCS bucket from which to read.")
var fObject = flag.String("object", "", "The object within which to read.")
var fReadSize = flag.Int("size", 1<<21, "The size of each read in bytes.")
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

// Make random reads within the given object, writing results to the supplied
// channel. Stop when the stop channel is closed.
func makeReads(
	o *gcs.Object,
	bucket gcs.Bucket,
	results chan<- result,
	stop <-chan struct{}) (err error)

func getBucket() (b gcs.Bucket, err error)

func getObject(bucket gcs.Bucket) (o *gcs.Object, err error)

// Run workers until SIGINT is received. Return a slice of results.
func runWorkers(
	o *gcs.Object,
	bucket gcs.Bucket) (results []result, err error) {
	b := syncutil.NewBundle(context.Background())

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
			err = makeReads(o, bucket, resultChan, stop)
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

func run() (err error) {
	// Open the bucket.
	bucket, err := getBucket()
	if err != nil {
		err = fmt.Errorf("getBucket: %v", err)
		return
	}

	// Stat the object.
	o, err := getObject(bucket)
	if err != nil {
		err = fmt.Errorf("getObject: %v", err)
		return
	}

	// Make reads.
	results, err := runWorkers(o, bucket)
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

	err := run()
	if err != nil {
		err = log.Fatal(err)
	}
}
