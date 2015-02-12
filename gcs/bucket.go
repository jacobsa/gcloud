// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"io"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

// Bucket represents a GCS bucket, pre-bound with a bucket name and necessary
// authorization information.
//
// Each method that may block accepts a context object that is used for
// deadlines and cancellation. Users need not package authorization information
// into the context object (using cloud.WithContext or similar).
type Bucket interface {
	Name() string

	// List the objects in the bucket that meet the criteria defined by the
	// query, returning a result object that contains the results and potentially
	// a cursor for retrieving the next portion of the larger set of results.
	ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error)

	// Create a reader for the contents of the object with the given name. The
	// caller must arrange for the reader to be closed when it is no longer
	// needed.
	NewReader(ctx context.Context, objectName string) (io.ReadCloser, error)

	// Return an ObjectWriter that can be used to create or overwrite an object
	// with the given attributes. attrs.Name must be specified. Otherwise, nil-
	// and zero-valud attributes are ignored.
	//
	// Object names must be valid non-empty UTF-8 no more than 1024 bytes in
	// length, not containing the code points U+000A (line feed) or U+000D
	// (carriage return).
	//
	// See here for authoritative documentation:
	//     https://cloud.google.com/storage/docs/bucket-naming#objectnames
	NewWriter(ctx context.Context, attrs *storage.ObjectAttrs) (ObjectWriter, error)

	// Delete the object with the given name.
	DeleteObject(ctx context.Context, name string) error
}

type bucket struct {
	projID string
	client *http.Client
	name   string
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.ListObjects(authContext, b.name, query)
}

func (b *bucket) NewReader(ctx context.Context, objectName string) (io.ReadCloser, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.NewReader(authContext, b.name, objectName)
}

func (b *bucket) NewWriter(ctx context.Context, attrs *storage.ObjectAttrs) (ObjectWriter, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)

	// Create and initialize the wrapped writer.
	wrapped := storage.NewWriter(authContext, b.name, attrs.Name)
	wrapped.ObjectAttrs = *attrs

	w := &objectWriter{
		wrapped: wrapped,
	}

	return w, nil
}

func (b *bucket) DeleteObject(ctx context.Context, name string) error {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.DeleteObject(authContext, b.name, name)
}
