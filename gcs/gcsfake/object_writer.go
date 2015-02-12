// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"bytes"

	"github.com/jacobsa/gcloud/gcs"
	"google.golang.org/cloud/storage"
)

type objectWriter struct {
	// The bucket to which we will commit ourselves when complete.
	bucket *bucket

	// User-supplied attributes. Always non-nil.
	attrs *storage.ObjectAttrs

	// When writing is successfully completed, an Object representing what was
	// written.
	object *storage.Object

	// The buffer to which we are forwarding writes.
	buf bytes.Buffer
}

func (w *objectWriter) Write(p []byte) (int, error)

func (w *objectWriter) Close() error

// TODO(jacobsa): We need integration test coverage for this.
func (w *objectWriter) Object() *storage.Object

func newObjectWriter(bucket *bucket, attrs *storage.ObjectAttrs) gcs.ObjectWriter
