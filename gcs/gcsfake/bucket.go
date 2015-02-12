// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create an in-memory bucket with the given name and empty contents.
func NewFakeBucket(name string) gcs.Bucket {
	b := &bucket{name: name}
	b.mu = syncutil.NewInvariantMutex(func() { b.checkInvariants() })
	return b
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
	mu   syncutil.InvariantMutex

	// The set of extant objects.
	//
	// INVARIANT: Strictly increasing.
	objects objectSlice // GUARDED_BY(mu)
}

// SHARED_LOCKS_REQUIRED(b.mu)
func (b *bucket) checkInvariants() {
	// Make sure 'objects' is strictly increasing.
	for i := 1; i < len(b.objects); i++ {
		objA := b.objects[i-1]
		objB := b.objects[i]
		if !(objA.metadata.Name < objB.metadata.Name) {
			panic(
				fmt.Sprintf(
					"Object names are not strictly increasing: %v vs. %v",
					objA.metadata.Name,
					objB.metadata.Name))
		}
	}
}

func (b *bucket) Name() string {
	return b.name
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) ListObjects(
	ctx context.Context,
	query *storage.Query) (listing *storage.Objects, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Set up the result object.
	listing = new(storage.Objects)

	// Handle nil queries.
	if query == nil {
		query = &storage.Query{}
	}

	// Handle defaults.
	maxResults := query.MaxResults
	if maxResults == 0 {
		maxResults = 1000
	}

	// Find where in the space of object names to start.
	nameStart := query.Prefix
	if query.Cursor != "" && query.Cursor > nameStart {
		nameStart = query.Cursor
	}

	// Find the range of indexes within the array to scan.
	indexStart := b.objects.lowerBound(nameStart)
	indexLimit := minInt(len(b.objects), indexStart+maxResults)

	// Scan the array.
	for i := indexStart; i < indexLimit; i++ {
		var o object = b.objects[i]

		// TODO(jacobsa): Handle prefixes.
		listing.Results = append(listing.Results, o.metadata)
	}

	// Set up a cursor for where to start the next scan if we didn't exhaust the
	// results.
	if indexLimit < len(b.objects) {
		listing.Next = &storage.Query{}
		*listing.Next = *query
		listing.Next.Cursor = b.objects[indexLimit].metadata.Name
	}

	return
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

// LOCKS_EXCLUDED(b.mu)
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
		Bucket:   b.Name(),
		Name:     attrs.Name,
		Owner:    "user-fake",
		Size:     int64(len(contents)),
		Metadata: attrs.Metadata,
	}

	// Set up contents.
	o.contents = contents

	return
}

// Add a record for an object with the given attributes and contents, then
// return the minted metadata.
//
// LOCKS_EXCLUDED(b.mu)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}
