// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// Tests registered by RegisterBucketTests.

package gcstesting

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"strings"
	"time"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func createObject(
	ctx context.Context,
	bucket gcs.Bucket,
	attrs *storage.ObjectAttrs,
	contents string) (o *storage.Object, err error) {
	// Create a writer.
	writer, err := bucket.NewWriter(ctx, attrs)
	if err != nil {
		return
	}

	// Copy into the writer.
	if _, err = io.Copy(writer, bytes.NewReader([]byte(contents))); err != nil {
		return
	}

	// Close the writer.
	if err = writer.Close(); err != nil {
		return
	}

	return writer.Object(), nil
}

func createEmpty(ctx context.Context, bucket gcs.Bucket, objectNames []string) error {
	bundle := syncutil.NewBundle(ctx)

	// Feed object names into a channel buffer.
	nameChan := make(chan string, len(objectNames))
	for _, n := range objectNames {
		nameChan <- n
	}

	close(nameChan)

	// Create in parallel.
	const parallelism = 10
	for i := 0; i < 10; i++ {
		bundle.Add(func(ctx context.Context) error {
			for objectName := range nameChan {
				attrs := &storage.ObjectAttrs{Name: objectName}
				if _, err := createObject(ctx, bucket, attrs, ""); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return bundle.Join()
}

// Convert from [16]byte to the slice type used by storage.Object.
func md5Sum(s string) []byte {
	array := md5.Sum([]byte(s))
	return array[:]
}

func computeCrc32C(s string) uint32 {
	return crc32.Checksum([]byte(s), crc32.MakeTable(crc32.Castagnoli))
}

////////////////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////////////////

type bucketTest struct {
	getBucket func() gcs.Bucket
	ctx       context.Context
	bucket    gcs.Bucket
}

var _ bucketTestSetUpInterface = &bucketTest{}

func (t *bucketTest) setUpBucketTest(b gcs.Bucket) {
	t.bucket = b
	t.ctx = context.Background()
}

func (t *bucketTest) createObject(name string, contents string) error {
	_, err := createObject(
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
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	o, err := createObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Check the Object struct.
	ExpectEq(t.bucket.Name(), o.Bucket)
	ExpectEq("foo", o.Name)
	ExpectEq("application/octet-stream", o.ContentType)
	ExpectEq("", o.ContentLanguage)
	ExpectEq("", o.CacheControl)
	ExpectThat(o.ACL, ElementsAre())
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
	ExpectThat(o.Deleted, DeepEquals(time.Time{}))
	ExpectLt(math.Abs(time.Since(o.Updated).Seconds()), 60)

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

	o, err := createObject(t.ctx, t.bucket, attrs, "taco")
	AssertEq(nil, err)

	// Check the Object struct.
	ExpectEq(t.bucket.Name(), o.Bucket)
	ExpectEq("foo", o.Name)
	ExpectEq("image/png", o.ContentType)
	ExpectEq("fr", o.ContentLanguage)
	ExpectEq("public", o.CacheControl)
	ExpectThat(o.ACL, ElementsAre())
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
	ExpectLt(math.Abs(time.Since(o.Updated).Seconds()), 60)

	// Make sure it matches what is in a listing.
	listing, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(listing.Prefixes, ElementsAre())
	AssertEq(nil, listing.Next)

	AssertEq(1, len(listing.Results))
	ExpectThat(listing.Results[0], DeepEquals(o))
}

func (t *createTest) WriteThenAbandon() {
	// Set up a writer for an object.
	attrs := &storage.ObjectAttrs{
		Name: "foo",
	}

	writer, err := t.bucket.NewWriter(t.ctx, attrs)
	AssertEq(nil, err)

	// Write a bunch of data, but don't yet close.
	_, err = io.Copy(writer, strings.NewReader(strings.Repeat("foo", 1<<19)))
	AssertEq(nil, err)

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

	// Make sure all and only the expected names exist.
	listingNames := make(map[string]struct{})
	for _, o := range objects.Results {
		listingNames[o.Name] = struct{}{}
	}

	expectedNames := make(map[string]struct{})
	for _, n := range names {
		expectedNames[n] = struct{}{}
	}

	for n, _ := range expectedNames {
		nameDump := hex.Dump([]byte(n))
		_, nameFound := listingNames[n]
		AssertTrue(nameFound, nameDump)
	}

	for n, _ := range listingNames {
		nameDump := hex.Dump([]byte(n))
		_, nameFound := expectedNames[n]
		AssertTrue(nameFound, nameDump)
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
	AssertTrue(false, "TODO")
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
	AssertTrue(false, "TODO")
}

func (t *listTest) Cursor() {
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
