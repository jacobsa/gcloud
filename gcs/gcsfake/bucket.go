// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcsfake

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Create an in-memory bucket with the given name and empty contents. The
// supplied clock will be used for generating timestamps.
func NewFakeBucket(clock timeutil.Clock, name string) gcs.Bucket {
	b := &bucket{clock: clock, name: name}
	b.mu = syncutil.NewInvariantMutex(b.checkInvariants)
	return b
}

type fakeObject struct {
	// A storage.Object representing a GCS entry for this object.
	entry storage.Object

	// The contents of the object. These never change.
	contents string
}

// A slice of objects compared by name.
type fakeObjectSlice []fakeObject

func (s fakeObjectSlice) Len() int {
	return len(s)
}

func (s fakeObjectSlice) Less(i, j int) bool {
	return s[i].entry.Name < s[j].entry.Name
}

func (s fakeObjectSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Return the smallest i such that s[i].entry.Name >= name, or len(s) if there
// is no such i.
func (s fakeObjectSlice) lowerBound(name string) int {
	pred := func(i int) bool {
		return s[i].entry.Name >= name
	}

	return sort.Search(len(s), pred)
}

// Return the smallest i such that s[i].entry.Name == name, or len(s) if there
// is no such i.
func (s fakeObjectSlice) find(name string) int {
	lb := s.lowerBound(name)
	if lb < len(s) && s[lb].entry.Name == name {
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

// Return the smallest i such that prefix < s[i].entry.Name and
// !strings.HasPrefix(s[i].entry.Name, prefix).
func (s fakeObjectSlice) prefixUpperBound(prefix string) int {
	successor := prefixSuccessor(prefix)
	if successor == "" {
		return len(s)
	}

	return s.lowerBound(successor)
}

type bucket struct {
	clock timeutil.Clock
	name  string
	mu    syncutil.InvariantMutex

	// The set of extant objects.
	//
	// INVARIANT: Strictly increasing.
	objects fakeObjectSlice // GUARDED_BY(mu)

	// The most recent generation number that was minted. The next object will
	// receive generation prevGeneration + 1.
	//
	// INVARIANT: This is an upper bound for generation numbers in objects.
	prevGeneration int64 // GUARDED_BY(mu)
}

// SHARED_LOCKS_REQUIRED(b.mu)
func (b *bucket) checkInvariants() {
	// Make sure 'objects' is strictly increasing.
	for i := 1; i < len(b.objects); i++ {
		objA := b.objects[i-1]
		objB := b.objects[i]
		if !(objA.entry.Name < objB.entry.Name) {
			panic(
				fmt.Sprintf(
					"Object names are not strictly increasing: %v vs. %v",
					objA.entry.Name,
					objB.entry.Name))
		}
	}

	// Make sure prevGeneration is an upper bound for object generation numbers.
	for _, o := range b.objects {
		if !(o.entry.Generation <= b.prevGeneration) {
			panic(
				fmt.Sprintf(
					"Object generation %v exceeds %v",
					o.entry.Generation,
					b.prevGeneration))
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
		var o fakeObject = b.objects[i]
		name := o.entry.Name

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

		// Otherwise, return as an object result. Make a copy to avoid handing back
		// internal state.
		var oCopy storage.Object = o.entry
		listing.Results = append(listing.Results, &oCopy)
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
			listing.Next.Cursor = b.objects[indexLimit].entry.Name
		}
	}

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) NewReader(
	ctx context.Context,
	req *gcs.ReadObjectRequest) (rc io.ReadCloser, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Find the object with the requested name.
	index := b.objects.find(req.Name)
	if index == len(b.objects) {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}

	o := b.objects[index]

	// Does the generation match?
	if req.Generation != 0 && req.Generation != o.entry.Generation {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf(
				"Object %s generation %v not found", req.Name, req.Generation),
		}

		return
	}

	rc = ioutil.NopCloser(strings.NewReader(o.contents))
	return
}

func (b *bucket) CreateObject(
	ctx context.Context,
	req *gcs.CreateObjectRequest) (o *storage.Object, err error) {
	// Check that the object name is legal.
	name := req.Attrs.Name
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

	// Snarf the object contents.
	buf := new(bytes.Buffer)
	if _, err = io.Copy(buf, req.Contents); err != nil {
		return
	}

	contents := buf.String()

	// Lock and proceed.
	b.mu.Lock()
	defer b.mu.Unlock()

	obj, err := b.addObjectLocked(req, contents)
	if err != nil {
		return
	}

	o = &obj
	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) StatObject(
	ctx context.Context,
	req *gcs.StatObjectRequest) (o *storage.Object, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Does the object exist?
	index := b.objects.find(req.Name)
	if index == len(b.objects) {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}

	// Make a copy to avoid handing back internal state.
	var objCopy storage.Object = b.objects[index].entry
	o = &objCopy

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) UpdateObject(
	ctx context.Context,
	req *gcs.UpdateObjectRequest) (o *storage.Object, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Match real GCS in not allowing the removal of ContentType.
	if req.ContentType != nil && *req.ContentType == "" {
		err = errors.New("The ContentType field is required and cannot be removed.")
		return
	}

	// Does the object exist?
	index := b.objects.find(req.Name)
	if index == len(b.objects) {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}

	var obj *storage.Object = &b.objects[index].entry

	// Update the entry's basic fields according to the request.
	if req.ContentType != nil {
		obj.ContentType = *req.ContentType
	}

	if req.ContentEncoding != nil {
		obj.ContentEncoding = *req.ContentEncoding
	}

	if req.ContentLanguage != nil {
		obj.ContentLanguage = *req.ContentLanguage
	}

	if req.CacheControl != nil {
		obj.CacheControl = *req.CacheControl
	}

	// Update the user metadata if necessary.
	if len(req.Metadata) > 0 {
		if obj.Metadata == nil {
			obj.Metadata = make(map[string]string)
		}

		for k, v := range req.Metadata {
			if v == nil {
				delete(obj.Metadata, k)
				continue
			}

			obj.Metadata[k] = *v
		}
	}

	// Bump up the entry generation number.
	obj.MetaGeneration++

	// Make a copy to avoid handing back internal state.
	var objCopy storage.Object = *obj
	o = &objCopy

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Do we possess the object with the given name?
	index := b.objects.find(name)
	if index == len(b.objects) {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", name),
		}

		return
	}

	// Remove the object.
	b.objects = append(b.objects[:index], b.objects[index+1:]...)

	return
}

// Create an object struct for the given attributes and contents.
//
// EXCLUSIVE_LOCKS_REQUIRED(b.mu)
func (b *bucket) mintObject(
	attrs *storage.ObjectAttrs,
	contents string) (o fakeObject) {
	// Set up basic info.
	b.prevGeneration++
	o.entry = storage.Object{
		Bucket:          b.Name(),
		Name:            attrs.Name,
		ContentType:     attrs.ContentType,
		ContentLanguage: attrs.ContentLanguage,
		CacheControl:    attrs.CacheControl,
		Owner:           "user-fake",
		Size:            int64(len(contents)),
		ContentEncoding: attrs.ContentEncoding,
		CRC32C:          crc32.Checksum([]byte(contents), crc32Table),
		MediaLink:       "http://localhost/download/storage/fake/" + attrs.Name,
		Metadata:        attrs.Metadata,
		Generation:      b.prevGeneration,
		MetaGeneration:  1,
		StorageClass:    "STANDARD",
		Updated:         b.clock.Now(),
	}

	// Fill in the MD5 field.
	md5Array := md5.Sum([]byte(contents))
	o.entry.MD5 = md5Array[:]

	// Set up contents.
	o.contents = contents

	// Match the real GCS client library's behavior of sniffing content types
	// when not explicitly specified.
	if o.entry.ContentType == "" {
		o.entry.ContentType = http.DetectContentType([]byte(contents))
	}

	return
}

// Add a record and return a copy of the minted entry.
//
// EXCLUSIVE_LOCKS_REQUIRED(b.mu)
func (b *bucket) addObjectLocked(
	req *gcs.CreateObjectRequest,
	contents string) (entry storage.Object, err error) {
	// Find any existing record for this name.
	existingIndex := b.objects.find(req.Attrs.Name)

	var existingRecord *fakeObject
	if existingIndex < len(b.objects) {
		existingRecord = &b.objects[existingIndex]
	}

	// Check preconditions.
	if req.GenerationPrecondition != nil {
		if *req.GenerationPrecondition == 0 && existingRecord != nil {
			err = &gcs.PreconditionError{
				Err: errors.New("Precondition failed: object exists"),
			}

			return
		}

		if *req.GenerationPrecondition > 0 {
			if existingRecord == nil {
				err = &gcs.PreconditionError{
					Err: errors.New("Precondition failed: object doesn't exist"),
				}

				return
			}

			existingGen := existingRecord.entry.Generation
			if existingGen != *req.GenerationPrecondition {
				err = &gcs.PreconditionError{
					Err: fmt.Errorf(
						"Precondition failed: object has generation %v",
						existingGen),
				}

				return
			}
		}
	}

	// Create an object record from the given attributes.
	var o fakeObject = b.mintObject(&req.Attrs, contents)

	// Replace an entry in or add an entry to our list of objects.
	if existingIndex < len(b.objects) {
		b.objects[existingIndex] = o
	} else {
		b.objects = append(b.objects, o)
		sort.Sort(b.objects)
	}

	entry = o.entry
	return
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
