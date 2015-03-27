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

package gcsutil

import (
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
)

// List all object names in the bucket into the supplied channel.
// Responsibility for closing the channel is not transferred.
func listIntoChannel(
	ctx context.Context,
	bucket gcs.Bucket,
	objectNames chan<- string) (err error) {
	req := &gcs.ListObjectsRequest{}
	for {
		// Call the bucket.
		var listing *gcs.Listing

		listing, err = bucket.ListObjects(ctx, req)
		if err != nil {
			return
		}

		// Send the names down the channel.
		for _, obj := range listing.Objects {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case objectNames <- obj.Name:
			}
		}

		// Are we done?
		if listing.ContinuationToken == "" {
			break
		}

		req.ContinuationToken = listing.ContinuationToken
	}

	return
}

// Delete all objects from the supplied bucket. Results are undefined if the
// bucket is being concurrently updated.
func DeleteAllObjects(
	ctx context.Context,
	bucket gcs.Bucket) error {
	bundle := syncutil.NewBundle(ctx)

	// List all of the objects in the bucket.
	objectNames := make(chan string, 100)
	bundle.Add(func(ctx context.Context) error {
		defer close(objectNames)
		return listIntoChannel(ctx, bucket, objectNames)
	})

	// Delete the objects in parallel.
	const parallelism = 64
	for i := 0; i < parallelism; i++ {
		bundle.Add(func(ctx context.Context) error {
			for objectName := range objectNames {
				if err := bucket.DeleteObject(ctx, objectName); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return bundle.Join()
}
