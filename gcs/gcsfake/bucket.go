// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"errors"
	"io"
	"sort"
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
	// A storage.Object representing metadata for this object. Never changes.
	metadata *storage.Object

	// The contents of the object. These never change.
	contents []byte
}

// A slice of objects compared by name.
type objectSlice []object

func (s objectSlice) Len() int
func (s objectSlice) Less(i, j int) bool
func (s objectSlice) Swap(i, j int)

type bucket struct {
	name string
	mu   sync.RWMutex

	// The set of extant objects.
	//
	// INVARIANT: Strictly increasing.
	objects objectSlice // GUARDED_BY(mu)
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

// Create an object struct for the given attributes and contents.
//
// EXCLUSIVE_LOCKS_REQUIRED(b.mu)
func (b *bucket) mintObject(
	attrs *storage.ObjectAttrs,
	contents []byte) object

func (b *bucket) addObject(
	attrs *storage.ObjectAttrs,
	contents []byte) *storage.Object {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create an object record from the given attributes.
	var o object = b.mintObject(attrs, contents)

	// Add it to our list of object.
	b.objects = append(b.objects, o)
	sort.Sort(b.objects)

	return o.metadata
}
