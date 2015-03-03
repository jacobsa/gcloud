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
	"google.golang.org/cloud/storage"
)

type ObjectInfo struct {
	Attrs    storage.ObjectAttrs
	Contents string
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
				o, err := CreateObject(ctx, bucket, &r.objectInfo.Attrs, r.objectInfo.Contents)
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
