// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsutil

import (
	"io"
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
	contents string) (o *storage.Object, err error) {
	// Create a writer.
	writer, err := bucket.NewWriter(ctx, attrs)
	if err != nil {
		return
	}

	// Copy into the writer.
	if _, err = io.Copy(writer, strings.NewReader(contents)); err != nil {
		return
	}

	// Close the writer.
	if err = writer.Close(); err != nil {
		return
	}

	o = writer.Object()
	return
}
