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

func (s objectSlice) Len() int           { return len(s) }
func (s objectSlice) Less(i, j int) bool { return s[i].metadata.Name < s[j].metadata.Name }
func (s objectSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Return the smallest i such that s[i].metadata.Name >= name, or len(s) if
// there is no such i.
func (s objectSlice) lowerBound(name string) int {
	pred := func(i int) bool {
		return s[i].metadata.Name >= name
	}

	return sort.Search(len(s), pred)
}

// Return the smallest i such that s[i].metadata.Name == name, or len(s) if
// there is no such i.
func (s objectSlice) find(name string) int {
	lb := s.lowerBound(name)
	if lb < len(s) && s[lb].metadata.Name == name {
		return lb
	}

	return len(s)
}

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

// LOCKS_EXCLUDED(mu)
func (b *bucket) DeleteObject(
	ctx context.Context,
	name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Do we possess the object with the given name?
	index := b.objects.find(name)
	if index == len(b.objects) {
		return errors.New("Object not found.")
	}

	// Remove the object.
	b.objects = append(b.objects[:index], b.objects[index+1:]...)

	return nil
}

// Create an object struct for the given attributes and contents.
//
// EXCLUSIVE_LOCKS_REQUIRED(b.mu)
func (b *bucket) mintObject(
	attrs *storage.ObjectAttrs,
	contents []byte) (o object) {
	// Set up metadata.
	// TODO(jacobsa): Other fields.
	o.metadata = &storage.Object{
		Bucket: b.Name(),
		Name:   attrs.Name,
	}

	// Set up contents.
	o.contents = contents

	return
}

// Add a record for an object with the given attributes and contents, then
// return the minted metadata.
//
// LOCKS_EXCLUDED(mu)
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
