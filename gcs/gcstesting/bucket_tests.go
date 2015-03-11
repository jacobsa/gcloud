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

// Tests registered by RegisterBucketTests.

package gcstesting

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"sort"
	"strings"
	"testing/iotest"
	"time"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	"github.com/jacobsa/gcsfuse/timeutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func createEmpty(
	ctx context.Context,
	bucket gcs.Bucket,
	objectNames []string) error {
	_, err := gcsutil.CreateEmptyObjects(ctx, bucket, objectNames)
	return err
}

// Convert from [16]byte to the slice type used by storage.Object.
func md5Sum(s string) []byte {
	array := md5.Sum([]byte(s))
	return array[:]
}

func computeCrc32C(s string) uint32 {
	return crc32.Checksum([]byte(s), crc32.MakeTable(crc32.Castagnoli))
}

func makeStringPtr(s string) *string {
	return &s
}

////////////////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////////////////

type bucketTest struct {
	ctx    context.Context
	bucket gcs.Bucket
	clock  timeutil.Clock
}

var _ bucketTestSetUpInterface = &bucketTest{}

func (t *bucketTest) setUpBucketTest(deps BucketTestDeps) {
	t.bucket = deps.Bucket
	t.clock = deps.Clock
	t.ctx = context.Background()
}

func (t *bucketTest) createObject(name string, contents string) error {
	_, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: name},
		contents)

	return err
}

func (t *bucketTest) readObject(objectName string) (contents string, err error) {
	// Open a reader.
	reader, err := t.bucket.NewReader(t.ctx, objectName)
	if err != nil {
		return
	}

	defer reader.Close()

	// Read the contents of the object.
	slice, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}

	// Transform to a string.
	contents = string(slice)

	return
}

// Ensure that the clock will report a different time after returning.
func (t *bucketTest) advanceTime()

// Return a matcher that matches event times as reported by the bucket
// corresponding to the supplied start time as measured by the test.
func (t *bucketTest) matchesStartTime(start time.Time) Matcher

////////////////////////////////////////////////////////////////////////
// Create
////////////////////////////////////////////////////////////////////////

type createTest struct {
	bucketTest
}

func (t *createTest) EmptyObject() {
	// Create the object.
	AssertEq(nil, t.createObject("foo", ""))

	// Ensure it shows up in a listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	AssertEq(1, len(objects.Results))
	o := objects.Results[0]

	AssertEq("foo", o.Name)
	ExpectEq(0, o.Size)
}

func (t *createTest) NonEmptyObject() {
	// Create the object.
	AssertEq(nil, t.createObject("foo", "taco"))

	// Ensure it shows up in a listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	AssertEq(1, len(objects.Results))
	o := objects.Results[0]

	AssertEq("foo", o.Name)
	ExpectEq(len("taco"), o.Size)
}

func (t *createTest) Overwrite() {
	// Create two versions of an object in sequence.
	AssertEq(nil, t.createObject("foo", "taco"))
	AssertEq(nil, t.createObject("foo", "burrito"))

	// The second version should show up in a listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	AssertEq(1, len(objects.Results))
	o := objects.Results[0]

	AssertEq("foo", o.Name)
	ExpectEq(len("burrito"), o.Size)

	// The second version should be what we get when we read the object.
	contents, err := t.readObject("foo")
	AssertEq(nil, err)
	ExpectEq("burrito", contents)
}

func (t *createTest) ObjectAttributes_Default() {
	// Create an object with default attributes aside from the name.
	createTime := t.clock.Now()
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	o, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Check the Object struct.
	ExpectEq(t.bucket.Name(), o.Bucket)
	ExpectEq("foo", o.Name)
	ExpectEq("text/plain; charset=utf-8", o.ContentType)
	ExpectEq("", o.ContentLanguage)
	ExpectEq("", o.CacheControl)
	ExpectThat(o.Owner, MatchesRegexp("^user-.*"))
	ExpectEq(len("taco"), o.Size)
	ExpectEq("", o.ContentEncoding)
	ExpectThat(o.MD5, DeepEquals(md5Sum("taco")))
	ExpectEq(computeCrc32C("taco"), o.CRC32C)
	ExpectThat(o.MediaLink, MatchesRegexp("download/storage.*foo"))
	ExpectEq(nil, o.Metadata)
	ExpectLt(0, o.Generation)
	ExpectEq(1, o.MetaGeneration)
	ExpectEq("STANDARD", o.StorageClass)
	ExpectThat(o.Deleted, timeutil.TimeEq(time.Time{}))
	ExpectThat(o.Updated, t.matchesStartTime(createTime))

	// Make sure it matches what is in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *createTest) ObjectAttributes_Explicit() {
	// Create an object with explicit attributes set.
	createTime := t.clock.Now()
	attrs := &storage.ObjectAttrs{
		Name:            "foo",
		ContentType:     "image/png",
		ContentLanguage: "fr",
		ContentEncoding: "gzip",
		CacheControl:    "public",
		Metadata: map[string]string{
			"foo": "bar",
			"baz": "qux",
		},
	}

	o, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Check the Object struct.
	ExpectEq(t.bucket.Name(), o.Bucket)
	ExpectEq("foo", o.Name)
	ExpectEq("image/png", o.ContentType)
	ExpectEq("fr", o.ContentLanguage)
	ExpectEq("public", o.CacheControl)
	ExpectThat(o.Owner, MatchesRegexp("^user-.*"))
	ExpectEq(len("taco"), o.Size)
	ExpectEq("gzip", o.ContentEncoding)
	ExpectThat(o.MD5, DeepEquals(md5Sum("taco")))
	ExpectEq(computeCrc32C("taco"), o.CRC32C)
	ExpectThat(o.MediaLink, MatchesRegexp("download/storage.*foo"))
	ExpectThat(o.Metadata, DeepEquals(attrs.Metadata))
	ExpectLt(0, o.Generation)
	ExpectEq(1, o.MetaGeneration)
	ExpectEq("STANDARD", o.StorageClass)
	ExpectThat(o.Deleted, DeepEquals(time.Time{}))
	ExpectThat(o.Deleted, timeutil.TimeEq(time.Time{}))
	ExpectThat(o.Updated, t.matchesStartTime(createTime))

	// Make sure it matches what is in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *createTest) ErrorAfterPartialContents() {
	const contents = "tacoburritoenchilada"

	// Set up a reader that will return some successful data, then an error.
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents: iotest.TimeoutReader(
			iotest.OneByteReader(
				strings.NewReader(contents))),
	}

	// An attempt to create the object should fail.
	_, err := t.bucket.CreateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("timeout")))

	// The object should not show up in a listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	ExpectThat(objects.Results, ElementsAre())
}

func (t *createTest) InterestingNames() {
	// Naming requirements:
	// Cf. https://cloud.google.com/storage/docs/bucket-naming
	const maxLegalLength = 1024

	names := []string{
		// Non-Roman scripts
		"타코",
		"世界",

		// Longest legal name
		strings.Repeat("a", maxLegalLength),

		// Line terminators besides CR and LF
		// Cf. https://en.wikipedia.org/wiki/Newline#Unicode
		"foo \u000b bar",
		"foo \u000c bar",
		"foo \u0085 bar",
		"foo \u2028 bar",
		"foo \u2029 bar",

		// Null byte.
		"foo \u0000 bar",

		// Non-control characters that are discouraged, but not forbidden,
		// according to the documentation.
		"foo # bar",
		"foo []*? bar",

		// Angstrom symbol singleton and normalized forms.
		// Cf. http://unicode.org/reports/tr15/
		"foo \u212b bar",
		"foo \u0041\u030a bar",
		"foo \u00c5 bar",

		// Hangul separating jamo
		// Cf. http://www.unicode.org/versions/Unicode7.0.0/ch18.pdf (Table 18-10)
		"foo \u3131\u314f bar",
		"foo \u1100\u1161 bar",
		"foo \uac00 bar",
	}

	var runes []rune

	// C0 control characters not forbidden by the docs.
	runes = nil
	for r := rune(0x01); r <= rune(0x1f); r++ {
		if r != '\u000a' && r != '\u000d' {
			runes = append(runes, r)
		}
	}

	names = append(names, fmt.Sprintf("foo %s bar", string(runes)))

	// C1 control characters, plus DEL.
	runes = nil
	for r := rune(0x7f); r <= rune(0x9f); r++ {
		runes = append(runes, r)
	}

	names = append(names, fmt.Sprintf("foo %s bar", string(runes)))

	// Make sure we can create each.
	for _, name := range names {
		nameDump := hex.Dump([]byte(name))

		err := t.createObject(name, "")
		AssertEq(nil, err, nameDump)
	}

	// Grab a listing and extract the names.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	var listingNames sort.StringSlice
	for _, o := range objects.Results {
		listingNames = append(listingNames, o.Name)
	}

	// The names should have come back sorted by their UTF-8 encodings.
	AssertTrue(sort.IsSorted(listingNames), "Names: %v", listingNames)

	// Make sure all and only the expected names exist.
	expectedNames := make(sort.StringSlice, len(names))
	copy(expectedNames, names)
	sort.Sort(expectedNames)

	ExpectThat(listingNames, DeepEquals(expectedNames))
}

func (t *createTest) IllegalNames() {
	// Naming requirements:
	// Cf. https://cloud.google.com/storage/docs/bucket-naming
	const maxLegalLength = 1024

	names := []string{
		// Empty and too long
		"",
		strings.Repeat("a", maxLegalLength+1),

		// Not valid UTF-8
		"foo\xff",

		// Carriage return and line feed
		"foo\u000abar",
		"foo\u000dbar",
	}

	// Make sure we cannot create any of the names above.
	for _, name := range names {
		nameDump := hex.Dump([]byte(name))

		err := t.createObject(name, "")
		AssertNe(nil, err, "Name:\n%s", nameDump)

		if name == "" {
			ExpectThat(err, Error(AnyOf(HasSubstr("Invalid"), HasSubstr("Required"))), nameDump)
		} else {
			ExpectThat(err, Error(HasSubstr("Invalid")), nameDump)
		}
	}

	// No objects should have been created.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)
	ExpectThat(objects.Results, ElementsAre())
}

func (t *createTest) GenerationPrecondition_Zero_Unsatisfied() {
	// Create an existing object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"taco")

	// Request to create another version of the object, with a precondition
	// saying it shouldn't exist. The request should fail.
	var gen int64 = 0
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents:               strings.NewReader("burrito"),
		GenerationPrecondition: &gen,
	}

	_, err = t.bucket.CreateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("Precondition")))

	// The old version should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	AssertEq("foo", listing.Results[0].Name)
	ExpectEq(o.Generation, listing.Results[0].Generation)
	ExpectEq(len("taco"), listing.Results[0].Size)

	// We should see the old contents when we read.
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("taco", string(contents))
}

func (t *createTest) GenerationPrecondition_Zero_Satisfied() {
	// Request to create an object with a precondition saying it shouldn't exist.
	// The request should succeed.
	var gen int64 = 0
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents:               strings.NewReader("burrito"),
		GenerationPrecondition: &gen,
	}

	o, err := t.bucket.CreateObject(t.ctx, req)
	AssertEq(nil, err)

	ExpectEq(len("burrito"), o.Size)
	ExpectNe(0, o.Generation)

	// The object should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	AssertEq("foo", listing.Results[0].Name)
	ExpectEq(o.Generation, listing.Results[0].Generation)
	ExpectEq(len("burrito"), listing.Results[0].Size)

	// We should see the new contents when we read.
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("burrito", string(contents))
}

func (t *createTest) GenerationPrecondition_NonZero_Unsatisfied_Missing() {
	// Request to create a non-existent object with a precondition saying it
	// should already exist with some generation number. The request should fail.
	var gen int64 = 17
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents:               strings.NewReader("burrito"),
		GenerationPrecondition: &gen,
	}

	_, err := t.bucket.CreateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("Precondition")))

	// Nothing should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)
	ExpectEq(0, len(listing.Results))
}

func (t *createTest) GenerationPrecondition_NonZero_Unsatisfied_Present() {
	// Create an existing object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"taco")

	// Request to create another version of the object, with a precondition for
	// the wrong generation. The request should fail.
	var gen int64 = o.Generation + 1
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents:               strings.NewReader("burrito"),
		GenerationPrecondition: &gen,
	}

	_, err = t.bucket.CreateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("Precondition")))

	// The old version should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	AssertEq("foo", listing.Results[0].Name)
	ExpectEq(o.Generation, listing.Results[0].Generation)
	ExpectEq(len("taco"), listing.Results[0].Size)

	// We should see the old contents when we read.
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("taco", string(contents))
}

func (t *createTest) GenerationPrecondition_NonZero_Satisfied() {
	// Create an existing object.
	orig, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"taco")

	// Request to create another version of the object, with a precondition
	// saying it should exist with the appropriate generation number. The request
	// should succeed.
	var gen int64 = orig.Generation
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents:               strings.NewReader("burrito"),
		GenerationPrecondition: &gen,
	}

	o, err := t.bucket.CreateObject(t.ctx, req)
	AssertEq(nil, err)

	ExpectEq(len("burrito"), o.Size)
	ExpectNe(orig.Generation, o.Generation)

	// The new version should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	AssertEq("foo", listing.Results[0].Name)
	ExpectEq(o.Generation, listing.Results[0].Generation)
	ExpectEq(len("burrito"), listing.Results[0].Size)

	// We should see the new contents when we read.
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("burrito", string(contents))
}

////////////////////////////////////////////////////////////////////////
// Read
////////////////////////////////////////////////////////////////////////

type readTest struct {
	bucketTest
}

func (t *readTest) NonExistentObject() {
	_, err := t.bucket.NewReader(t.ctx, "foobar")

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("object doesn't exist")))
}

func (t *readTest) EmptyObject() {
	// Create
	AssertEq(nil, t.createObject("foo", ""))

	// Read
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("", string(contents))

	// Close
	AssertEq(nil, r.Close())
}

func (t *readTest) NonEmptyObject() {
	// Create
	AssertEq(nil, t.createObject("foo", "taco"))

	// Read
	r, err := t.bucket.NewReader(t.ctx, "foo")
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("taco", string(contents))

	// Close
	AssertEq(nil, r.Close())
}

////////////////////////////////////////////////////////////////////////
// Stat
////////////////////////////////////////////////////////////////////////

type statTest struct {
	bucketTest
}

func (t *statTest) NonExistentObject() {
	req := &gcs.StatObjectRequest{
		Name: "foo",
	}

	_, err := t.bucket.StatObject(t.ctx, req)
	ExpectEq(gcs.ErrNotFound, err)
}

func (t *statTest) StatAfterCreating() {
	// Create an object.
	createTime := t.clock.Now()
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	orig, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)
	AssertThat(orig.Updated, t.matchesStartTime(createTime))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Stat it.
	req := &gcs.StatObjectRequest{
		Name: "foo",
	}

	o, err := t.bucket.StatObject(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, o)

	ExpectEq("foo", o.Name)
	ExpectEq(orig.Generation, o.Generation)
	ExpectEq(len("taco"), o.Size)
	ExpectThat(o.Deleted, timeutil.TimeEq(time.Time{}))
	ExpectThat(o.Updated, timeutil.TimeEq(orig.Updated))
}

func (t *statTest) StatAfterOverwriting() {
	// Create an object.
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Overwrite it.
	overwriteTime := t.clock.Now()
	o2, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "burrito")
	AssertEq(nil, err)
	AssertThat(o2.Updated, t.matchesStartTime(overwriteTime))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Stat it.
	req := &gcs.StatObjectRequest{
		Name: "foo",
	}

	o, err := t.bucket.StatObject(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, o)

	ExpectEq("foo", o.Name)
	ExpectEq(o2.Generation, o.Generation)
	ExpectEq(len("burrito"), o.Size)
	ExpectThat(o.Deleted, timeutil.TimeEq(time.Time{}))
	ExpectThat(o.Updated, timeutil.TimeEq(o2.Updated))
}

func (t *statTest) StatAfterUpdating() {
	// Create an object.
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Update it.
	updateTime := t.clock.Now()
	ureq := &gcs.UpdateObjectRequest{
		Name:        "foo",
		ContentType: makeStringPtr("image/png"),
	}

	o2, err := t.bucket.UpdateObject(t.ctx, ureq)
	AssertEq(nil, err)
	AssertThat(o2.Updated, t.matchesStartTime(updateTime))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Stat it.
	req := &gcs.StatObjectRequest{
		Name: "foo",
	}

	o, err := t.bucket.StatObject(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, o)

	ExpectEq("foo", o.Name)
	ExpectEq(o2.Generation, o.Generation)
	ExpectEq(o2.MetaGeneration, o.MetaGeneration)
	ExpectEq(len("taco"), o.Size)
	ExpectThat(o.Deleted, timeutil.TimeEq(time.Time{}))
	ExpectThat(o.Updated, timeutil.TimeEq(o2.Updated))
}

////////////////////////////////////////////////////////////////////////
// Update
////////////////////////////////////////////////////////////////////////

type updateTest struct {
	bucketTest
}

func (t *updateTest) NonExistentObject() {
	req := &gcs.UpdateObjectRequest{
		Name:        "foo",
		ContentType: makeStringPtr("image/png"),
	}

	_, err := t.bucket.UpdateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(MatchesRegexp("404|Object not found")))
}

func (t *updateTest) RemoveContentType() {
	// Create an object.
	attrs := &storage.ObjectAttrs{
		Name:        "foo",
		ContentType: "image/png",
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Attempt to remove the content type field.
	req := &gcs.UpdateObjectRequest{
		Name:        "foo",
		ContentType: makeStringPtr(""),
	}

	_, err = t.bucket.UpdateObject(t.ctx, req)

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("required")))
}

func (t *updateTest) RemoveAllFields() {
	// Create an object with explicit attributes set.
	attrs := &storage.ObjectAttrs{
		Name:            "foo",
		ContentType:     "image/png",
		ContentEncoding: "gzip",
		ContentLanguage: "fr",
		CacheControl:    "public",
		Metadata: map[string]string{
			"foo": "bar",
		},
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Remove all of the fields that were set, aside from user metadata and
	// ContentType (which cannot be removed).
	req := &gcs.UpdateObjectRequest{
		Name:            "foo",
		ContentEncoding: makeStringPtr(""),
		ContentLanguage: makeStringPtr(""),
		CacheControl:    makeStringPtr(""),
	}

	o, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Check the returned object.
	AssertEq("foo", o.Name)
	AssertEq(len("taco"), o.Size)
	AssertEq(2, o.MetaGeneration)

	ExpectEq("image/png", o.ContentType)
	ExpectEq("", o.ContentEncoding)
	ExpectEq("", o.ContentLanguage)
	ExpectEq("", o.CacheControl)

	ExpectThat(o.Metadata, DeepEquals(attrs.Metadata))

	// Check that a listing agrees.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *updateTest) ModifyAllFields() {
	// Create an object with explicit attributes set.
	attrs := &storage.ObjectAttrs{
		Name:            "foo",
		ContentType:     "image/png",
		ContentEncoding: "gzip",
		ContentLanguage: "fr",
		CacheControl:    "public",
		Metadata: map[string]string{
			"foo": "bar",
		},
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Modify all of the fields that were set, aside from user metadata.
	req := &gcs.UpdateObjectRequest{
		Name:            "foo",
		ContentType:     makeStringPtr("image/jpeg"),
		ContentEncoding: makeStringPtr("bzip2"),
		ContentLanguage: makeStringPtr("de"),
		CacheControl:    makeStringPtr("private"),
	}

	o, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Check the returned object.
	AssertEq("foo", o.Name)
	AssertEq(len("taco"), o.Size)
	AssertEq(2, o.MetaGeneration)

	ExpectEq("image/jpeg", o.ContentType)
	ExpectEq("bzip2", o.ContentEncoding)
	ExpectEq("de", o.ContentLanguage)
	ExpectEq("private", o.CacheControl)

	ExpectThat(o.Metadata, DeepEquals(attrs.Metadata))

	// Check that a listing agrees.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *updateTest) MixedModificationsToFields() {
	// Create an object with some explicit attributes set.
	attrs := &storage.ObjectAttrs{
		Name:            "foo",
		ContentType:     "image/png",
		ContentEncoding: "gzip",
		ContentLanguage: "fr",
		Metadata: map[string]string{
			"foo": "bar",
		},
	}

	_, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Leave one field unmodified, delete one field, modify an existing field,
	// and add a new field.
	req := &gcs.UpdateObjectRequest{
		Name:            "foo",
		ContentType:     nil,
		ContentEncoding: makeStringPtr(""),
		ContentLanguage: makeStringPtr("de"),
		CacheControl:    makeStringPtr("private"),
	}

	o, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Check the returned object.
	AssertEq("foo", o.Name)
	AssertEq(len("taco"), o.Size)
	AssertEq(2, o.MetaGeneration)

	ExpectEq("image/png", o.ContentType)
	ExpectEq("", o.ContentEncoding)
	ExpectEq("de", o.ContentLanguage)
	ExpectEq("private", o.CacheControl)

	ExpectThat(o.Metadata, DeepEquals(attrs.Metadata))

	// Check that a listing agrees.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *updateTest) AddUserMetadata() {
	// Create an object with no user metadata.
	attrs := &storage.ObjectAttrs{
		Name:     "foo",
		Metadata: nil,
	}

	orig, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	AssertEq(nil, orig.Metadata)

	// Add some metadata.
	req := &gcs.UpdateObjectRequest{
		Name: "foo",
		Metadata: map[string]*string{
			"0": makeStringPtr("taco"),
			"1": makeStringPtr("burrito"),
		},
	}

	o, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Check the returned object.
	AssertEq("foo", o.Name)
	AssertEq(len("taco"), o.Size)
	AssertEq(2, o.MetaGeneration)

	ExpectThat(
		o.Metadata,
		DeepEquals(
			map[string]string{
				"0": "taco",
				"1": "burrito",
			}))

	// Check that a listing agrees.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *updateTest) MixedModificationsToUserMetadata() {
	// Create an object with some user metadata.
	attrs := &storage.ObjectAttrs{
		Name: "foo",
		Metadata: map[string]string{
			"0": "taco",
			"2": "enchilada",
			"3": "queso",
		},
	}

	orig, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	AssertThat(orig.Metadata, DeepEquals(attrs.Metadata))

	// Leave an existing field untouched, add a new field, remove an existing
	// field, and modify an existing field.
	req := &gcs.UpdateObjectRequest{
		Name: "foo",
		Metadata: map[string]*string{
			"1": makeStringPtr("burrito"),
			"2": nil,
			"3": makeStringPtr("updated"),
		},
	}

	o, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Check the returned object.
	AssertEq("foo", o.Name)
	AssertEq(len("taco"), o.Size)
	AssertEq(2, o.MetaGeneration)

	ExpectThat(
		o.Metadata,
		DeepEquals(
			map[string]string{
				"0": "taco",
				"1": "burrito",
				"3": "updated",
			}))

	// Check that a listing agrees.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

////////////////////////////////////////////////////////////////////////
// Delete
////////////////////////////////////////////////////////////////////////

type deleteTest struct {
	bucketTest
}

func (t *deleteTest) NonExistentObject() {
	err := t.bucket.DeleteObject(t.ctx, "foobar")

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("Not Found")))
}

func (t *deleteTest) Successful() {
	// Create an object.
	AssertEq(nil, t.createObject("a", "taco"))

	// Delete it.
	AssertEq(nil, t.bucket.DeleteObject(t.ctx, "a"))

	// It shouldn't show up in a listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)
	ExpectThat(objects.Results, ElementsAre())

	// It shouldn't be readable.
	_, err = t.bucket.NewReader(t.ctx, "a")

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("object doesn't exist")))
}

////////////////////////////////////////////////////////////////////////
// List
////////////////////////////////////////////////////////////////////////

type listTest struct {
	bucketTest
}

func (t *listTest) EmptyBucket() {
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	ExpectThat(objects.Results, ElementsAre())
	ExpectThat(objects.Prefixes, ElementsAre())
	ExpectEq(nil, objects.Next)
}

func (t *listTest) NewlyCreatedObject() {
	// Create an object.
	AssertEq(nil, t.createObject("a", "taco"))

	// List all objects in the bucket.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	var o *storage.Object
	AssertEq(1, len(objects.Results))

	// a
	o = objects.Results[0]
	AssertEq("a", o.Name)
	ExpectEq(t.bucket.Name(), o.Bucket)
	ExpectEq(len("taco"), o.Size)
}

func (t *listTest) TrivialQuery() {
	// Create few objects.
	AssertEq(nil, t.createObject("a", "taco"))
	AssertEq(nil, t.createObject("b", "burrito"))
	AssertEq(nil, t.createObject("c", "enchilada"))

	// List all objects in the bucket.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	var o *storage.Object
	AssertEq(3, len(objects.Results))

	// a
	o = objects.Results[0]
	AssertEq("a", o.Name)
	ExpectEq(len("taco"), o.Size)

	// b
	o = objects.Results[1]
	AssertEq("b", o.Name)
	ExpectEq(len("burrito"), o.Size)

	// c
	o = objects.Results[2]
	AssertEq("c", o.Name)
	ExpectEq(len("enchilada"), o.Size)
}

func (t *listTest) Delimiter_SingleRune() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"!",
				"a",
				"b",
				"b!foo",
				"b!bar",
				"b!baz!qux",
				"c!",
				"d!taco",
				"d!burrito",
				"e",
			}))

	// List with the delimiter "!".
	query := &storage.Query{
		Delimiter: "!",
	}

	objects, err := t.bucket.ListObjects(t.ctx, query)
	AssertEq(nil, err)
	AssertNe(nil, objects)
	AssertEq(nil, objects.Next)

	// Prefixes
	ExpectThat(objects.Prefixes, ElementsAre("!", "b!", "c!", "d!"))

	// Objects
	AssertEq(3, len(objects.Results))

	ExpectEq("a", objects.Results[0].Name)
	ExpectEq("b", objects.Results[1].Name)
	ExpectEq("e", objects.Results[2].Name)
}

func (t *listTest) Delimiter_MultiRune() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"!",
				"!!",
				"!!!",
				"!!!!",
				"!!!!!!!!!",
				"a",
				"b",
				"b!",
				"b!foo",
				"b!!",
				"b!!!",
				"b!!foo",
				"b!!!foo",
				"b!!bar",
				"b!!baz!!qux",
				"c!!",
				"d!!taco",
				"d!!burrito",
				"e",
			}))

	// List with the delimiter "!!".
	query := &storage.Query{
		Delimiter: "!!",
	}

	objects, err := t.bucket.ListObjects(t.ctx, query)
	AssertEq(nil, err)
	AssertNe(nil, objects)
	AssertEq(nil, objects.Next)

	// Prefixes
	ExpectThat(objects.Prefixes, ElementsAre("!!", "b!!", "c!!", "d!!"))

	// Objects
	AssertEq(6, len(objects.Results))

	ExpectEq("!", objects.Results[0].Name)
	ExpectEq("a", objects.Results[1].Name)
	ExpectEq("b", objects.Results[2].Name)
	ExpectEq("b!", objects.Results[3].Name)
	ExpectEq("b!foo", objects.Results[4].Name)
	ExpectEq("e", objects.Results[5].Name)
}

func (t *listTest) Prefix() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"a",
				"a\x7f",
				"b",
				"b\x00",
				"b\x01",
				"b타코",
				"c",
			}))

	// List with the prefix "b".
	query := &storage.Query{
		Prefix: "b",
	}

	objects, err := t.bucket.ListObjects(t.ctx, query)
	AssertEq(nil, err)
	AssertNe(nil, objects)
	AssertEq(nil, objects.Next)
	AssertThat(objects.Prefixes, ElementsAre())

	// Objects
	AssertEq(4, len(objects.Results))

	ExpectEq("b", objects.Results[0].Name)
	ExpectEq("b\x00", objects.Results[1].Name)
	ExpectEq("b\x01", objects.Results[2].Name)
	ExpectEq("b타코", objects.Results[3].Name)
}

func (t *listTest) PrefixAndDelimiter_SingleRune() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"blag",
				"blag!",
				"blah",
				"blah!a",
				"blah!a\x7f",
				"blah!b",
				"blah!b!",
				"blah!b!asd",
				"blah!b\x00",
				"blah!b\x00!",
				"blah!b\x00!asd",
				"blah!b\x00!asd!sdf",
				"blah!b\x01",
				"blah!b\x01!",
				"blah!b\x01!asd",
				"blah!b\x01!asd!sdf",
				"blah!b타코",
				"blah!b타코!",
				"blah!b타코!asd",
				"blah!b타코!asd!sdf",
				"blah!c",
			}))

	// List with the prefix "blah!b" and the delimiter "!".
	query := &storage.Query{
		Prefix:    "blah!b",
		Delimiter: "!",
	}

	objects, err := t.bucket.ListObjects(t.ctx, query)
	AssertEq(nil, err)
	AssertNe(nil, objects)
	AssertEq(nil, objects.Next)

	// Prefixes
	ExpectThat(
		objects.Prefixes,
		ElementsAre(
			"blah!b\x00!",
			"blah!b\x01!",
			"blah!b!",
			"blah!b타코!",
		))

	// Objects
	AssertEq(4, len(objects.Results))

	ExpectEq("blah!b", objects.Results[0].Name)
	ExpectEq("blah!b\x00", objects.Results[1].Name)
	ExpectEq("blah!b\x01", objects.Results[2].Name)
	ExpectEq("blah!b타코", objects.Results[3].Name)
}

func (t *listTest) PrefixAndDelimiter_MultiRune() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"blag",
				"blag!!",
				"blah",
				"blah!!a",
				"blah!!a\x7f",
				"blah!!b",
				"blah!!b!",
				"blah!!b!!",
				"blah!!b!!asd",
				"blah!!b\x00",
				"blah!!b\x00!",
				"blah!!b\x00!!",
				"blah!!b\x00!!asd",
				"blah!!b\x00!!asd!sdf",
				"blah!!b\x01",
				"blah!!b\x01!",
				"blah!!b\x01!!",
				"blah!!b\x01!!asd",
				"blah!!b\x01!!asd!sdf",
				"blah!!b타코",
				"blah!!b타코!",
				"blah!!b타코!!",
				"blah!!b타코!!asd",
				"blah!!b타코!!asd!sdf",
				"blah!!c",
			}))

	// List with the prefix "blah!b" and the delimiter "!".
	query := &storage.Query{
		Prefix:    "blah!!b",
		Delimiter: "!!",
	}

	objects, err := t.bucket.ListObjects(t.ctx, query)
	AssertEq(nil, err)
	AssertNe(nil, objects)
	AssertEq(nil, objects.Next)

	// Prefixes
	ExpectThat(
		objects.Prefixes,
		ElementsAre(
			"blah!!b\x00!!",
			"blah!!b\x01!!",
			"blah!!b!!",
			"blah!!b타코!!",
		))

	// Objects
	AssertEq(8, len(objects.Results))

	ExpectEq("blah!!b", objects.Results[0].Name)
	ExpectEq("blah!!b\x00", objects.Results[1].Name)
	ExpectEq("blah!!b\x00!", objects.Results[2].Name)
	ExpectEq("blah!!b\x01", objects.Results[3].Name)
	ExpectEq("blah!!b\x01!", objects.Results[4].Name)
	ExpectEq("blah!!b!", objects.Results[5].Name)
	ExpectEq("blah!!b타코", objects.Results[6].Name)
	ExpectEq("blah!!b타코!", objects.Results[7].Name)
}

func (t *listTest) Cursor_BucketEndsWithRunOfIndividualObjects() {
	// Create a good number of objects, containing a run of objects sharing a
	// prefix under the delimiter "!".
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"a",
				"b",
				"c",
				"c!0",
				"c!1",
				"c!2",
				"c!3",
				"c!4",
				"d!",
				"e",
				"e!",
				"f!",
				"g!",
				"h",
			}))

	// List repeatedly with a small value for MaxResults. Keep track of all of
	// the objects and prefixes we find.
	query := &storage.Query{
		Delimiter:  "!",
		MaxResults: 2,
	}

	var objects []string
	var prefixes []string

	for query != nil {
		res, err := t.bucket.ListObjects(t.ctx, query)
		AssertEq(nil, err)

		for _, o := range res.Results {
			objects = append(objects, o.Name)
		}

		for _, p := range res.Prefixes {
			prefixes = append(prefixes, p)
		}

		query = res.Next
	}

	// Check the results.
	ExpectThat(
		objects,
		ElementsAre(
			"a",
			"b",
			"c",
			"e",
			"h",
		))

	ExpectThat(
		prefixes,
		ElementsAre(
			"c!",
			"d!",
			"e!",
			"f!",
			"g!",
		))
}

func (t *listTest) Cursor_BucketEndsWithRunOfObjectsGroupedByDelimiter() {
	// Create a good number of objects, containing runs of objects sharing a
	// prefix under the delimiter "!" at the end of the bucket.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"a",
				"b",
				"c",
				"c!",
				"c!0",
				"c!1",
				"c!2",
				"d!",
				"d!0",
				"d!1",
				"d!2",
			}))

	// List repeatedly with a small value for MaxResults. Keep track of all of
	// the objects and prefixes we find.
	query := &storage.Query{
		Delimiter:  "!",
		MaxResults: 2,
	}

	var objects []string
	var prefixes []string

	for query != nil {
		res, err := t.bucket.ListObjects(t.ctx, query)
		AssertEq(nil, err)

		for _, o := range res.Results {
			objects = append(objects, o.Name)
		}

		for _, p := range res.Prefixes {
			prefixes = append(prefixes, p)
		}

		query = res.Next
	}

	// Check the results.
	ExpectThat(
		objects,
		ElementsAre(
			"a",
			"b",
			"c",
		))

	ExpectThat(
		prefixes,
		ElementsAre(
			"c!",
			"d!",
		))
}
