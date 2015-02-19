// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

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
	"time"
	"unicode/utf8"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Create an in-memory bucket with the given name and empty contents.
func NewFakeBucket(name string) gcs.Bucket {
	b := &bucket{name: name}
	b.mu = syncutil.NewInvariantMutex(func() { b.checkInvariants() })
	return b
}

type fakeObject struct {
	// A storage.Object representing a GCS entry for this object.
	entry *storage.Object

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
	name string
	mu   syncutil.InvariantMutex

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

		// Otherwise, save as an object result.
		listing.Results = append(listing.Results, o.entry)
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
	objectName string) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	index := b.objects.find(objectName)
	if index == len(b.objects) {
		return nil, errors.New("object doesn't exist.")
	}

	return ioutil.NopCloser(strings.NewReader(b.objects[index].contents)), nil
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

	// Store the object.
	// TODO(jacobsa): This object might be concurrently modified. Return a copy.
	o = b.addObject(&req.Attrs, contents)

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) UpdateObject(
	ctx context.Context,
	req *gcs.UpdateObjectRequest) (o *storage.Object, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Does the object exist?
	index := b.objects.find(req.Name)
	if index == len(b.objects) {
		err = errors.New("Object Not Found.")
		return
	}

	var obj *storage.Object = b.objects[index].entry

	// Update the object according to the request.
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

	// Make a copy.
	o = new(storage.Object)
	*o = *obj

	return
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
		return errors.New("Object Not Found.")
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
	contents string) (o fakeObject) {
	// Set up basic info.
	b.prevGeneration++
	o.entry = &storage.Object{
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
		Updated:         time.Now(),
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

// Add a record for an object with the given attributes and contents, then
// return the minted entry.
//
// LOCKS_EXCLUDED(b.mu)
func (b *bucket) addObject(
	attrs *storage.ObjectAttrs,
	contents string) *storage.Object {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create an object record from the given attributes.
	var o fakeObject = b.mintObject(attrs, contents)

	// Replace an entry in or add an entry to our list of objects.
	existingIndex := b.objects.find(attrs.Name)
	if existingIndex < len(b.objects) {
		b.objects[existingIndex] = o
	} else {
		b.objects = append(b.objects, o)
		sort.Sort(b.objects)
	}

	return o.entry
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
