// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

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
	// Object names must:
	//
	// *  be non-empty.
	// *  be no longer than 1024 bytes.
	// *  be valid UTF-8.
	// *  not contain the code point U+000A (line feed).
	// *  not contain the code point U+000D (carriage return).
	//
	// See here for authoritative documentation:
	//     https://cloud.google.com/storage/docs/bucket-naming#objectnames
	NewWriter(ctx context.Context, attrs *storage.ObjectAttrs) (ObjectWriter, error)

	// TODO(jacobsa): Comments.
	UpdateObject(
		ctx context.Context,
		newAttrs *storage.ObjectAttrs) (*storage.Object, error)

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

func (b *bucket) NewWriter(
	ctx context.Context,
	attrs *storage.ObjectAttrs) (ObjectWriter, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)

	// As of 2015-02, the wrapped storage package doesn't check this for us,
	// causing silently transformed names:
	//     https://github.com/GoogleCloudPlatform/gcloud-golang/issues/111
	if !utf8.ValidString(attrs.Name) {
		return nil, errors.New("Invalid object name: not valid UTF-8")
	}

	// Create and initialize the wrapped writer.
	wrapped := storage.NewWriter(authContext, b.name, attrs.Name)
	wrapped.ObjectAttrs = *attrs

	w := &objectWriter{
		wrapped: wrapped,
	}

	return w, nil
}

func (b *bucket) UpdateObject(
	ctx context.Context,
	newAttrs *storage.ObjectAttrs) (*storage.Object, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.UpdateAttrs(authContext, b.Name(), newAttrs.Name, *newAttrs)
}

func (b *bucket) DeleteObject(ctx context.Context, name string) error {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.DeleteObject(authContext, b.name, name)
}
