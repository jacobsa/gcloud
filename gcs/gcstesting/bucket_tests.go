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
	"unicode"

	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	"github.com/jacobsa/gcloud/syncutil"
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
	err := gcsutil.CreateEmptyObjects(ctx, bucket, objectNames)
	return err
}

// Convert from [16]byte to the slice type used by gcs.Object.
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

// Return a list of object names that might be problematic for GCS or the Go
// client but are nevertheless documented to be legal.
//
// Useful links:
//
//     https://cloud.google.com/storage/docs/bucket-naming
//     http://www.unicode.org/Public/7.0.0/ucd/UnicodeData.txt
//     http://www.unicode.org/versions/Unicode7.0.0/ch02.pdf (Table 2-3)
//
func interestingNames() (names []string) {
	const maxLegalLength = 1024

	names = []string{
		// Characters specifically mentioned by RFC 3986, i.e. that might be
		// important in URL encoding/decoding.
		"foo : bar",
		"foo / bar",
		"foo ? bar",
		"foo # bar",
		"foo [ bar",
		"foo ] bar",
		"foo @ bar",
		"foo ! bar",
		"foo $ bar",
		"foo & bar",
		"foo ' bar",
		"foo ( bar",
		"foo ) bar",
		"foo * bar",
		"foo + bar",
		"foo , bar",
		"foo ; bar",
		"foo = bar",
		"foo - bar",
		"foo . bar",
		"foo _ bar",
		"foo ~ bar",

		// Other tricky URL cases.
		"foo () bar",
		"foo [] bar",
		"foo // bar",
		"foo %?/ bar",
		"foo http://google.com/search?q=foo&bar=baz#qux bar",

		"foo ?bar",
		"foo? bar",
		"foo/ bar",
		"foo /bar",

		// Non-Roman scripts
		"타코",
		"世界",

		// Longest legal name
		strings.Repeat("a", maxLegalLength),

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

		// Unicode specials
		// Cf. http://en.wikipedia.org/wiki/Specials_%28Unicode_block%29
		"foo \ufff9 bar",
		"foo \ufffa bar",
		"foo \ufffb bar",
		"foo \ufffc bar",
		"foo \ufffd bar",
	}

	// All codepoints in Unicode general categories C* (control and special) and
	// Z* (space), except for:
	//
	//  *  Cn (non-character and reserved), which is not included in unicode.C.
	//  *  Co (private usage), which is large.
	//  *  Cs (surrages), which is large.
	//  *  U+000A and U+000D, which are forbidden by the docs.
	//
	for r := rune(0); r <= unicode.MaxRune; r++ {
		// TODO(jacobsa): Re-enable these runes once GCS is fixed or the
		// documentation is updated.
		// See: https://github.com/jacobsa/gcloud/issues/2
		if r == 0x85 || r == 0x2028 || r == 0x2029 {
			continue
		}

		if !unicode.In(r, unicode.C) && !unicode.In(r, unicode.Z) {
			continue
		}

		if unicode.In(r, unicode.Co) {
			continue
		}

		if unicode.In(r, unicode.Cs) {
			continue
		}

		if r == 0x0a || r == 0x0d {
			continue
		}

		names = append(names, fmt.Sprintf("foo %s bar", string(r)))
	}

	return
}

// Given lists of strings A and B, return those values that are in A but not in
// B. If A contains duplicates of a value V not in B, the only guarantee is
// that V is returned at least once.
func listDifference(a []string, b []string) (res []string) {
	// This is slow, but more obviously correct than the fast algorithm.
	m := make(map[string]struct{})
	for _, s := range b {
		m[s] = struct{}{}
	}

	for _, s := range a {
		if _, ok := m[s]; !ok {
			res = append(res, s)
		}
	}

	return
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
		name,
		contents)

	return err
}

func (t *bucketTest) readObject(objectName string) (contents string, err error) {
	// Open a reader.
	req := &gcs.ReadObjectRequest{
		Name: objectName,
	}

	reader, err := t.bucket.NewReader(t.ctx, req)
	if err != nil {
		return
	}

	defer func() {
		AssertEq(nil, reader.Close())
	}()

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
func (t *bucketTest) advanceTime() {
	// For simulated clocks, we can just advance the time.
	if c, ok := t.clock.(*timeutil.SimulatedClock); ok {
		c.AdvanceTime(time.Second)
		return
	}

	// Otherwise, sleep a moment.
	time.Sleep(time.Millisecond)
}

// Return a matcher that matches event times as reported by the bucket
// corresponding to the supplied start time as measured by the test.
func (t *bucketTest) matchesStartTime(start time.Time) Matcher {
	// For simulated clocks we can use exact equality.
	if _, ok := t.clock.(*timeutil.SimulatedClock); ok {
		return timeutil.TimeEq(start)
	}

	// Otherwise, we need to take into account latency between the start of our
	// call and the time the server actually executed the operation.
	const slop = 60 * time.Second
	return timeutil.TimeNear(start, slop)
}

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
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	o := listing.Results[0]

	AssertEq("foo", o.Name)
	ExpectEq(0, o.Size)
}

func (t *createTest) NonEmptyObject() {
	// Create the object.
	AssertEq(nil, t.createObject("foo", "taco"))

	// Ensure it shows up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	o := listing.Results[0]

	AssertEq("foo", o.Name)
	ExpectEq(len("taco"), o.Size)
}

func (t *createTest) Overwrite() {
	// Create two versions of an object in sequence.
	AssertEq(nil, t.createObject("foo", "taco"))
	AssertEq(nil, t.createObject("foo", "burrito"))

	// The second version should show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	o := listing.Results[0]

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
	ExpectEq("application/octet-stream", o.ContentType)
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
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	ExpectThat(listing.Results, ElementsAre())
}

func (t *createTest) InterestingNames() {
	// Grab a list of interesting legal names.
	names := interestingNames()

	// Set up a function that invokes another function for each object name, with
	// some degree of parallelism.
	const parallelism = 32 // About 300 ms * 100 Hz
	forEachName := func(f func(context.Context, string)) {
		b := syncutil.NewBundle(t.ctx)

		// Feed names.
		nameChan := make(chan string)
		b.Add(func(ctx context.Context) error {
			defer close(nameChan)
			for _, n := range names {
				nameChan <- n
			}
			return nil
		})

		// Consume names.
		for i := 0; i < parallelism; i++ {
			b.Add(func(ctx context.Context) error {
				for n := range nameChan {
					f(ctx, n)
				}
				return nil
			})
		}

		b.Join()
	}

	// Make sure we can create each name.
	forEachName(func(ctx context.Context, name string) {
		err := t.createObject(name, name)
		ExpectEq(nil, err, "Failed to create:\n%s", hex.Dump([]byte(name)))
	})

	// Make sure we can read each, and that we get back the content we created
	// above.
	forEachName(func(ctx context.Context, name string) {
		contents, err := t.readObject(name)
		ExpectEq(nil, err, "Failed to read:\n%s", hex.Dump([]byte(name)))
		if err == nil {
			ExpectEq(name, contents, "Incorrect contents:\n%s", hex.Dump([]byte(name)))
		}
	})

	// Grab a listing and extract the names.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	var listingNames []string
	for _, o := range listing.Results {
		listingNames = append(listingNames, o.Name)
	}

	// The names should have come back sorted by their UTF-8 encodings.
	AssertTrue(sort.IsSorted(sort.StringSlice(listingNames)))

	// Make sure all and only the expected names exist.
	if diff := listDifference(listingNames, names); len(diff) != 0 {
		var dumps []string
		for _, n := range diff {
			dumps = append(dumps, hex.Dump([]byte(n)))
		}

		AddFailure(
			"Unexpected names in listing:\n%s",
			strings.Join(dumps, "\n"))
	}

	if diff := listDifference(names, listingNames); len(diff) != 0 {
		var dumps []string
		for _, n := range diff {
			dumps = append(dumps, hex.Dump([]byte(n)))
		}

		AddFailure(
			"Names missing from listing:\n%s",
			strings.Join(dumps, "\n"))
	}
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
			ExpectThat(
				err,
				Error(AnyOf(HasSubstr("Invalid"), HasSubstr("Required"))),
				"Name:\n%s",
				nameDump)
		} else {
			ExpectThat(err, Error(HasSubstr("Invalid")), "Name:\n%s", nameDump)
		}
	}

	// No objects should have been created.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)
	ExpectThat(listing.Results, ElementsAre())
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

	AssertThat(err, HasSameTypeAs(&gcs.PreconditionError{}))
	ExpectThat(err, Error(MatchesRegexp("object exists|googleapi.*412")))

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
	contents, err := t.readObject("foo")
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
	contents, err := t.readObject("foo")
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

	AssertThat(err, HasSameTypeAs(&gcs.PreconditionError{}))
	ExpectThat(err, Error(MatchesRegexp("object doesn't exist|googleapi.*412")))

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

	AssertThat(err, HasSameTypeAs(&gcs.PreconditionError{}))
	ExpectThat(err, Error(MatchesRegexp("generation|googleapi.*412")))

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
	contents, err := t.readObject("foo")
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
	contents, err := t.readObject("foo")
	AssertEq(nil, err)
	ExpectEq("burrito", string(contents))
}

////////////////////////////////////////////////////////////////////////
// Read
////////////////////////////////////////////////////////////////////////

type readTest struct {
	bucketTest
}

func (t *readTest) ObjectNameDoesntExist() {
	req := &gcs.ReadObjectRequest{
		Name: "foobar",
	}

	_, err := t.bucket.NewReader(t.ctx, req)

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("(?i)not found|404")))
}

func (t *readTest) EmptyObject() {
	// Create
	AssertEq(nil, t.createObject("foo", ""))

	// Read
	req := &gcs.ReadObjectRequest{
		Name: "foo",
	}

	r, err := t.bucket.NewReader(t.ctx, req)
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
	req := &gcs.ReadObjectRequest{
		Name: "foo",
	}

	r, err := t.bucket.NewReader(t.ctx, req)
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("taco", string(contents))

	// Close
	AssertEq(nil, r.Close())
}

func (t *readTest) ParticularGeneration_NeverExisted() {
	// Create an object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"")

	AssertEq(nil, err)
	AssertGt(o.Generation, 0)

	// Attempt to read a different generation.
	req := &gcs.ReadObjectRequest{
		Name:       "foo",
		Generation: o.Generation + 1,
	}

	_, err = t.bucket.NewReader(t.ctx, req)

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("(?i)not found|404")))
}

func (t *readTest) ParticularGeneration_HasBeenDeleted() {
	// Create an object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"")

	AssertEq(nil, err)
	AssertGt(o.Generation, 0)

	// Delete it.
	err = t.bucket.DeleteObject(t.ctx, "foo")
	AssertEq(nil, err)

	// Attempt to read by that generation.
	req := &gcs.ReadObjectRequest{
		Name:       "foo",
		Generation: o.Generation,
	}

	_, err = t.bucket.NewReader(t.ctx, req)

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("(?i)not found|404")))
}

func (t *readTest) ParticularGeneration_Exists() {
	// Create an object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"taco")

	AssertEq(nil, err)
	AssertGt(o.Generation, 0)

	// Attempt to read the correct generation.
	req := &gcs.ReadObjectRequest{
		Name:       "foo",
		Generation: o.Generation,
	}

	r, err := t.bucket.NewReader(t.ctx, req)
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("taco", string(contents))

	// Close
	AssertEq(nil, r.Close())
}

func (t *readTest) ParticularGeneration_ObjectHasBeenOverwritten() {
	// Create an object.
	o, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"taco")

	AssertEq(nil, err)
	AssertGt(o.Generation, 0)

	// Overwrite with a new generation.
	o2, err := gcsutil.CreateObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: "foo"},
		"burrito")

	AssertEq(nil, err)
	AssertGt(o2.Generation, 0)
	AssertNe(o.Generation, o2.Generation)

	// Reading by the old generation should fail.
	req := &gcs.ReadObjectRequest{
		Name:       "foo",
		Generation: o.Generation,
	}

	_, err = t.bucket.NewReader(t.ctx, req)

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("(?i)not found|404")))

	// Reading by the new generation should work.
	req.Generation = o2.Generation

	r, err := t.bucket.NewReader(t.ctx, req)
	AssertEq(nil, err)

	contents, err := ioutil.ReadAll(r)
	AssertEq(nil, err)
	ExpectEq("burrito", string(contents))

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

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("not found|doesn't exist")))
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
	createTime := t.clock.Now()
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	orig, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)
	AssertThat(orig.Updated, t.matchesStartTime(createTime))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Update the object.
	ureq := &gcs.UpdateObjectRequest{
		Name:        "foo",
		ContentType: makeStringPtr("image/png"),
	}

	o2, err := t.bucket.UpdateObject(t.ctx, ureq)
	AssertEq(nil, err)
	AssertNe(o2.MetaGeneration, orig.MetaGeneration)

	// Despite the name, 'Updated' doesn't reflect object updates, only creation
	// of new generations. Cf. Google-internal bug 19684518.
	AssertThat(o2.Updated, timeutil.TimeEq(orig.Updated))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Stat the object.
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

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("not found|404")))
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

func (t *updateTest) DoesntAffectUpdateTime() {
	// Create an object.
	createTime := t.clock.Now()
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	o, err := gcsutil.CreateObject(t.ctx, t.bucket, attrs, "")
	AssertEq(nil, err)
	AssertThat(o.Updated, t.matchesStartTime(createTime))

	// Ensure the time below doesn't match exactly.
	t.advanceTime()

	// Modify a field.
	req := &gcs.UpdateObjectRequest{
		Name:        "foo",
		ContentType: makeStringPtr("image/jpeg"),
	}

	o2, err := t.bucket.UpdateObject(t.ctx, req)
	AssertEq(nil, err)

	// Despite the name, 'Updated' doesn't reflect object updates, only creation
	// of new generations. Cf. Google-internal bug 19684518.
	ExpectThat(o2.Updated, timeutil.TimeEq(o.Updated))
}

////////////////////////////////////////////////////////////////////////
// Delete
////////////////////////////////////////////////////////////////////////

type deleteTest struct {
	bucketTest
}

func (t *deleteTest) NonExistentObject() {
	err := t.bucket.DeleteObject(t.ctx, "foobar")

	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
	ExpectThat(err, Error(MatchesRegexp("not found|404")))
}

func (t *deleteTest) Successful() {
	// Create an object.
	AssertEq(nil, t.createObject("a", "taco"))

	// Delete it.
	AssertEq(nil, t.bucket.DeleteObject(t.ctx, "a"))

	// It shouldn't show up in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, listing)
	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)
	ExpectThat(listing.Results, ElementsAre())

	// It shouldn't be readable.
	req := &gcs.ReadObjectRequest{
		Name: "a",
	}

	_, err = t.bucket.NewReader(t.ctx, req)
	ExpectThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
}

////////////////////////////////////////////////////////////////////////
// List
////////////////////////////////////////////////////////////////////////

type listTest struct {
	bucketTest
}

func (t *listTest) EmptyBucket() {
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, listing)
	ExpectThat(listing.Results, ElementsAre())
	ExpectThat(listing.Prefixes, ElementsAre())
	ExpectEq(nil, listing.Next)
}

func (t *listTest) NewlyCreatedObject() {
	// Create an object.
	AssertEq(nil, t.createObject("a", "taco"))

	// List all objects in the bucket.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, listing)
	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	var o *gcs.Object
	AssertEq(1, len(listing.Results))

	// a
	o = listing.Results[0]
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
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, listing)
	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	var o *gcs.Object
	AssertEq(3, len(listing.Results))

	// a
	o = listing.Results[0]
	AssertEq("a", o.Name)
	ExpectEq(len("taco"), o.Size)

	// b
	o = listing.Results[1]
	AssertEq("b", o.Name)
	ExpectEq(len("burrito"), o.Size)

	// c
	o = listing.Results[2]
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
	req := &gcs.ListObjectsRequest{
		Delimiter: "!",
	}

	listing, err := t.bucket.ListObjects(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, listing)
	AssertEq(nil, listing.Next)

	// Prefixes
	ExpectThat(listing.Prefixes, ElementsAre("!", "b!", "c!", "d!"))

	// Objects
	AssertEq(3, len(listing.Results))

	ExpectEq("a", listing.Results[0].Name)
	ExpectEq("b", listing.Results[1].Name)
	ExpectEq("e", listing.Results[2].Name)
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
	req := &gcs.ListObjectsRequest{
		Delimiter: "!!",
	}

	listing, err := t.bucket.ListObjects(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, listing)
	AssertEq(nil, listing.Next)

	// Prefixes
	ExpectThat(listing.Prefixes, ElementsAre("!!", "b!!", "c!!", "d!!"))

	// Objects
	AssertEq(6, len(listing.Results))

	ExpectEq("!", listing.Results[0].Name)
	ExpectEq("a", listing.Results[1].Name)
	ExpectEq("b", listing.Results[2].Name)
	ExpectEq("b!", listing.Results[3].Name)
	ExpectEq("b!foo", listing.Results[4].Name)
	ExpectEq("e", listing.Results[5].Name)
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
	req := &gcs.ListObjectsRequest{
		Prefix: "b",
	}

	listing, err := t.bucket.ListObjects(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, listing)
	AssertEq(nil, listing.Next)
	AssertThat(listing.Prefixes, ElementsAre())

	// Objects
	AssertEq(4, len(listing.Results))

	ExpectEq("b", listing.Results[0].Name)
	ExpectEq("b\x00", listing.Results[1].Name)
	ExpectEq("b\x01", listing.Results[2].Name)
	ExpectEq("b타코", listing.Results[3].Name)
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
	req := &gcs.ListObjectsRequest{
		Prefix:    "blah!b",
		Delimiter: "!",
	}

	listing, err := t.bucket.ListObjects(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, listing)
	AssertEq(nil, listing.Next)

	// Prefixes
	ExpectThat(
		listing.Prefixes,
		ElementsAre(
			"blah!b\x00!",
			"blah!b\x01!",
			"blah!b!",
			"blah!b타코!",
		))

	// Objects
	AssertEq(4, len(listing.Results))

	ExpectEq("blah!b", listing.Results[0].Name)
	ExpectEq("blah!b\x00", listing.Results[1].Name)
	ExpectEq("blah!b\x01", listing.Results[2].Name)
	ExpectEq("blah!b타코", listing.Results[3].Name)
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
	req := &gcs.ListObjectsRequest{
		Prefix:    "blah!!b",
		Delimiter: "!!",
	}

	listing, err := t.bucket.ListObjects(t.ctx, req)
	AssertEq(nil, err)
	AssertNe(nil, listing)
	AssertEq(nil, listing.Next)

	// Prefixes
	ExpectThat(
		listing.Prefixes,
		ElementsAre(
			"blah!!b\x00!!",
			"blah!!b\x01!!",
			"blah!!b!!",
			"blah!!b타코!!",
		))

	// Objects
	AssertEq(8, len(listing.Results))

	ExpectEq("blah!!b", listing.Results[0].Name)
	ExpectEq("blah!!b\x00", listing.Results[1].Name)
	ExpectEq("blah!!b\x00!", listing.Results[2].Name)
	ExpectEq("blah!!b\x01", listing.Results[3].Name)
	ExpectEq("blah!!b\x01!", listing.Results[4].Name)
	ExpectEq("blah!!b!", listing.Results[5].Name)
	ExpectEq("blah!!b타코", listing.Results[6].Name)
	ExpectEq("blah!!b타코!", listing.Results[7].Name)
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
	req := &gcs.ListObjectsRequest{
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
	req := &gcs.ListObjectsRequest{
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
