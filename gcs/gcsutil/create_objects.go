// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsutil

import (
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

type ObjectInfo struct {
	attrs    storage.ObjectAttrs
	contents string
}

// Create multiple objects with some parallelism, returning corresponding
// storage.Object records for the objects created.
func CreateObjects(
	ctx context.Context,
	bucket gcs.Bucket,
	input []*ObjectInfo) (objects []*storage.Object, err error) {
	bundle := syncutil.NewBundle(ctx)

	// Size the output slice.
	objects = make([]*storage.Object, len(input))

	// Feed ObjectInfo records into a channel.
	type record struct {
		index      int
		objectInfo *ObjectInfo
	}

	recordChan := make(chan record, len(input))
	for i, o := range input {
		recordChan <- record{i, o}
	}

	close(recordChan)

	// Create the objects in parallel, writing to the output slice as we go.
	const parallelism = 64
	for i := 0; i < 10; i++ {
		bundle.Add(func(ctx context.Context) error {
			for r := range recordChan {
				o, err := CreateObject(ctx, bucket, &r.objectInfo.attrs, r.objectInfo.contents)
				if err != nil {
					return err
				}

				objects[r.index] = o
			}

			return nil
		})
	}

	err = bundle.Join()
	return
}
