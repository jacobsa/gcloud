// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"errors"
	"io"
	"sync"

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create an in-memory bucket with the given name and empty contents.
func NewFakeBucket(name string) gcs.Bucket {
	return &bucket{name: name}
}

type object struct {
	// The attributes with which this object was created. These never change.
	attrs *storage.ObjectAttrs

	// The contents of the object. These never change.
	contents []byte
}

type bucket struct {
	name string
	mu   sync.RWMutex

	// The set of extant objects.
	//
	// INVARIANT: Strictly increasing by object.attrs.Name.
	objects []object // GUARDED_BY(mu)
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) ListObjects(
	ctx context.Context,
	query *storage.Query) (*storage.Objects, error) {
	return nil, errors.New("TODO: Implement ListObjects.")
}

func (b *bucket) NewReader(
	ctx context.Context,
	objectName string) (io.ReadCloser, error) {
	return nil, errors.New("TODO: Implement NewReader.")
}

func (b *bucket) NewWriter(
	ctx context.Context,
	attrs *storage.ObjectAttrs) (gcs.ObjectWriter, error) {
	return newObjectWriter(b, attrs), nil
}

func (b *bucket) DeleteObject(
	ctx context.Context,
	name string) error {
	return errors.New("TODO: Implement DeleteObject.")
}
