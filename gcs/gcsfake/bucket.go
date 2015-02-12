// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"unicode/utf8"

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
	contents string
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

// Return the smallest string that is lexicographically larger than prefix and
// does not have prefix as a prefix. For the sole case where this is not
// possible (all strings consisting solely of 0xff bytes, including the empty
// string), return the empty string.
func prefixSuccessor(prefix string) string {
	// Attempt to increment the last byte. If that is a 0xff byte, erase it and
	// recurse. If we hit an empty string, then we know our task is impossible.
	limit := []byte(prefix)
	for len(limit) > 0 {
		b := limit[len(limit)-1]
		if b != 0xff {
			limit[len(limit)-1]++
			break
		}

		limit = limit[:len(limit)-1]
	}

	return string(limit)
}

// Return the smallest i such that prefix < s[i].metadata.Name and
// !strings.HasPrefix(s[i].metadata.Name, prefix).
func (s objectSlice) prefixUpperBound(prefix string) int {
	successor := prefixSuccessor(prefix)
	if successor == "" {
		return len(s)
	}

	return s.lowerBound(successor)
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
	prefixLimit := b.objects.prefixUpperBound(query.Prefix)
	indexLimit := minInt(indexStart+maxResults, prefixLimit)

	// Scan the array.
	var lastResultWasPrefix bool
	for i := indexStart; i < indexLimit; i++ {
		var o object = b.objects[i]
		name := o.metadata.Name

		// Search for a delimiter if necessary.
		if query.Delimiter != "" {
			// Search only in the part after the prefix.
			nameMinusQueryPrefix := name[len(query.Prefix):]

			delimiterIndex := strings.Index(nameMinusQueryPrefix, query.Delimiter)
			if delimiterIndex >= 0 {
				resultPrefixLimit := delimiterIndex

				// Transform to an index within name.
				resultPrefixLimit += len(query.Prefix)

				// Include the delimiter in the result.
				resultPrefixLimit += len(query.Delimiter)

				// Save the result, but only if it's not a duplicate.
				resultPrefix := name[:resultPrefixLimit]
				if len(listing.Prefixes) == 0 ||
					listing.Prefixes[len(listing.Prefixes)-1] != resultPrefix {
					listing.Prefixes = append(listing.Prefixes, resultPrefix)
				}

				lastResultWasPrefix = true
				continue
			}
		}

		lastResultWasPrefix = false

		// Otherwise, save as an object result.
		listing.Results = append(listing.Results, o.metadata)
	}

	// Set up a cursor for where to start the next scan if we didn't exhaust the
	// results.
	if indexLimit < prefixLimit {
		listing.Next = &storage.Query{}
		*listing.Next = *query

		// Ion is if the final object we visited was returned as an element in
		// listing.Prefixes, we want to skip all other objects that would result in
		// the same so we don't return duplicate elements in listing.Prefixes
		// accross requests.
		if lastResultWasPrefix {
			lastResultPrefix := listing.Prefixes[len(listing.Prefixes)-1]
			listing.Next.Cursor = prefixSuccessor(lastResultPrefix)

			// Check an assumption: prefixSuccessor cannot result in the empty string
			// above because object names must be non-empty UTF-8 strings, and there
			// is no valid non-empty UTF-8 string that consists of entirely 0xff
			// bytes.
			if listing.Next.Cursor == "" {
				err = errors.New("Unexpected empty string from prefixSuccessor")
				return
			}
		} else {
			// Otherwise, we'll start scanning at the next object.
			listing.Next.Cursor = b.objects[indexLimit].metadata.Name
		}
	}

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) NewReader(
	ctx context.Context,
	objectName string) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	index := b.objects.find(objectName)
	if index == len(b.objects) {
		return nil, errors.New("Object not found.")
	}

	return ioutil.NopCloser(strings.NewReader(b.objects[index].contents)), nil
}

func (b *bucket) NewWriter(
	ctx context.Context,
	attrs *storage.ObjectAttrs) (gcs.ObjectWriter, error) {
	// Check that the object name is legal.
	name := attrs.Name
	if len(name) == 0 || len(name) > 1024 {
		return nil, errors.New("Invalid object name: length must be in [1, 1024]")
	}

	if !utf8.ValidString(name) {
		return nil, errors.New("Invalid object name: not valid UTF-8")
	}

	for _, r := range name {
		if r == 0x0a || r == 0x0d {
			return nil, errors.New("Invalid object name: must not contain CR or LF")
		}
	}

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
	contents string) (o object) {
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
	contents string) *storage.Object {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create an object record from the given attributes.
	var o object = b.mintObject(attrs, contents)

	// Replace an entry in or add an entry to our list of objects.
	existingIndex := b.objects.find(attrs.Name)
	if existingIndex < len(b.objects) {
		b.objects[existingIndex] = o
	} else {
		b.objects = append(b.objects, o)
		sort.Sort(b.objects)
	}

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
