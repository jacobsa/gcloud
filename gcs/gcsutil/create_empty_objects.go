// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsutil

import (
	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create empty objects with default attributes for all of the supplied names.
func CreateEmptyObjects(
	ctx context.Context,
	bucket gcs.Bucket,
	names []string) ([]*storage.Object, error) {
	// Set up a list of ObjectInfo structs.
	infoStructs := make([]*ObjectInfo, len(names))
	for i, name := range names {
		infoStructs[i] = &ObjectInfo{
			Attrs: storage.ObjectAttrs{
				Name: name,
			},
		}
	}

	// Defer to CreateObjects.
	return CreateObjects(ctx, bucket, infoStructs)
}
