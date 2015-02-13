// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsutil

import (
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// List all object names in the bucket into the supplied channel.
// Responsibility for closing the channel is not transferred.
func listIntoChannel(
	ctx context.Context,
	bucket gcs.Bucket,
	objectNames chan<- string) error {
	query := &storage.Query{}
	for query != nil {
		objects, err := bucket.ListObjects(ctx, query)
		if err != nil {
			return err
		}

		for _, obj := range objects.Results {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case objectNames <- obj.Name:
			}
		}

		query = objects.Next
	}

	return nil
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
