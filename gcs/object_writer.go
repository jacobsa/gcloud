// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"io"

	"google.golang.org/cloud/storage"
)

// An interface for creating objects within a GCS bucket. The user writes the
// object's contents via the Write method, then calls Close. The new object is
// not visible (and any previous version is not overwritten) until Close is
// called.
//
// If Close is successful, metadata about the object will be available via the
// Object method.
type ObjectWriter interface {
	io.WriteCloser

	// Return metadata about the successfully-written object. Must be called only
	// if Close() was called and was successful.
	Object() *storage.Object
}

type objectWriter struct {
	wrapped *storage.Writer
}

func (w *objectWriter) Write(p []byte) (int, error) {
	return w.wrapped.Write(p)
}

func (w *objectWriter) Close() error {
	return w.wrapped.Close()
}

func (w *objectWriter) Object() *storage.Object {
	return w.wrapped.Object()
}
