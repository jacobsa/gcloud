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

	// The buffer to which we are forwarding writes. Nil after Close has been
	// called.
	buf *bytes.Buffer
}

func (w *objectWriter) Write(p []byte) (int, error) {
	if w.buf == nil {
		panic("Call to Write after call to Close.")
	}

	return w.buf.Write(p)
}

func (w *objectWriter) Close() error {
	// Consume the buffer.
	if w.buf == nil {
		panic("Extra call to Close.")
	}

	contents := w.buf.Bytes()
	w.buf = nil

	// Commit the contents to the bucket, initializing w.object.
	w.bucket.addObject(w.object)

	return nil
}

func (w *objectWriter) Object() *storage.Object {
	if w.object == nil {
		panic("Call to Object() before successful Close().")
	}

	return w.object
}

func newObjectWriter(bucket *bucket, attrs *storage.ObjectAttrs) gcs.ObjectWriter {
	return &objectWriter{
		bucket: bucket,
		attrs:  attrs,
		buf:    new(bytes.Buffer),
	}
}
