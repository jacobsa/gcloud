// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsutil

import (
	"strings"

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create an object with the supplied contents in the given bucket. attrs.Name
// must be set; all other fields are optional.
func CreateObject(
	ctx context.Context,
	bucket gcs.Bucket,
	attrs *storage.ObjectAttrs,
	contents string) (*storage.Object, error) {
	req := &gcs.CreateObjectRequest{
		Attrs:    *attrs,
		Contents: strings.NewReader(contents),
	}

	return bucket.CreateObject(ctx, req)
}
