// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// An integration test that uses the real GCS. Run it with appropriate flags as
// follows:
//
//     go test -bucket <bucket name>
//
// The bucket's contents are not preserved.
//
// The first time you run the test, it may die with a URL to visit to obtain an
// authorization code after authorizing the test to access your bucket. Run it
// again with the "-auth_code" flag afterward.

package gcs_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/oauthutil"
	"github.com/jacobsa/gcloud/syncutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	storagev1 "google.golang.org/api/storage/v1"
	"google.golang.org/cloud/storage"
)

func TestOgletest(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Wiring code
////////////////////////////////////////////////////////////////////////

var fBucket = flag.String("bucket", "", "Empty bucket to use for storage.")
var fAuthCode = flag.String("auth_code", "", "Auth code from GCS console.")
var fDebugHttp = flag.Bool("debug_http", false, "Dump information about HTTP requests.")

func getHttpClientOrDie() *http.Client {
	// Set up a token source.
	config := &oauth2.Config{
		ClientID:     "517659276674-k9tr62f5rpd1k6ivvhadq0etbu4gu3t5.apps.googleusercontent.com",
		ClientSecret: "A6Xo63GDMRHmZ2TB7CO99lLN",
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
		Scopes:       []string{storagev1.DevstorageFull_controlScope},
		Endpoint:     google.Endpoint,
	}

	tokenSource, err := oauthutil.NewTerribleTokenSource(
		config,
		flag.Lookup("auth_code"),
		".gcs_integration_test.token_cache.json")

	if err != nil {
		log.Fatalln("oauthutil.NewTerribleTokenSource:", err)
	}

	// Ensure that we fail early if misconfigured, by requesting an initial
	// token.
	if _, err := tokenSource.Token(); err != nil {
		log.Fatalln("Getting initial OAuth token:", err)
	}

	// Create the HTTP transport.
	transport := &oauth2.Transport{
		Source: tokenSource,
		Base:   http.DefaultTransport,
	}

	if *fDebugHttp {
		transport.Base = &debuggingTransport{wrapped: transport.Base}
	}

	return &http.Client{Transport: transport}
}

func getBucketNameOrDie() string {
	s := *fBucket
	if s == "" {
		log.Fatalln("You must set --bucket.")
	}

	return s
}

// Return a bucket based on the contents of command-line flags, exiting the
// process if misconfigured.
func getBucketOrDie() gcs.Bucket {
	// A project ID is apparently only needed for creating and listing buckets,
	// presumably since a bucket ID already maps to a unique project ID (cf.
	// http://goo.gl/Plh3rb). This doesn't currently matter to us.
	const projectId = "some_project_id"

	// Set up a GCS connection.
	conn, err := gcs.NewConn(projectId, getHttpClientOrDie())
	if err != nil {
		log.Fatalf("gcs.NewConn: %v", err)
	}

	// Open the bucket.
	return conn.GetBucket(getBucketNameOrDie())
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// List all object names in the bucket into the supplied channel.
// Responsibility for closing the channel is not accepted.
func listIntoChannel(ctx context.Context, b gcs.Bucket, objectNames chan<- string) error {
	query := &storage.Query{}
	for query != nil {
		objects, err := b.ListObjects(ctx, query)
		if err != nil {
			return err
		}

		for _, obj := range objects.Results {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case objectNames <- obj.Name:
			}
		}

		query = objects.Next
	}

	return nil
}

// Delete everything in the bucket, exiting the process on failure.
func deleteAllObjectsOrDie(ctx context.Context, b gcs.Bucket) {
	bundle := syncutil.NewBundle(ctx)

	// List all of the objects in the bucket.
	objectNames := make(chan string, 10)
	bundle.Add(func(ctx context.Context) error {
		defer close(objectNames)
		return listIntoChannel(ctx, b, objectNames)
	})

	// Delete the objects in parallel.
	const parallelism = 10
	for i := 0; i < parallelism; i++ {
		bundle.Add(func(ctx context.Context) error {
			for objectName := range objectNames {
				if err := b.DeleteObject(ctx, objectName); err != nil {
					return err
				}
			}

			return nil
		})
	}

	// Wait.
	err := bundle.Join()
	if err != nil {
		panic("deleteAllObjectsOrDie: " + err.Error())
	}
}

func createObject(
	ctx context.Context,
	bucket gcs.Bucket,
	attrs *storage.ObjectAttrs,
	contents string) error {
	// Create a writer.
	writer, err := bucket.NewWriter(ctx, attrs)
	if err != nil {
		return err
	}

	// Copy into the writer.
	_, err = io.Copy(writer, bytes.NewReader([]byte(contents)))

	// Close the writer.
	return writer.Close()
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
				if err := createObject(ctx, bucket, attrs, ""); err != nil {
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
// HTTP debugging
////////////////////////////////////////////////////////////////////////

func readAllAndClose(rc io.ReadCloser) string {
	// Read.
	contents, err := ioutil.ReadAll(rc)
	if err != nil {
		panic(err)
	}

	// Close.
	if err := rc.Close(); err != nil {
		panic(err)
	}

	return string(contents)
}

// Read everything from *rc, then replace it with a copy.
func snarfBody(rc *io.ReadCloser) string {
	contents := readAllAndClose(*rc)
	*rc = ioutil.NopCloser(bytes.NewBufferString(contents))
	return contents
}

type debuggingTransport struct {
	wrapped http.RoundTripper
}

func (t *debuggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Print information about the request.
	fmt.Println("========== REQUEST ===========")
	fmt.Println(req.Method, req.URL, req.Proto)
	for k, vs := range req.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if req.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&req.Body))
	}

	// Execute the request.
	res, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return res, err
	}

	// Print the response.
	fmt.Println("========== RESPONSE ==========")
	fmt.Println(res.Proto, res.Status)
	for k, vs := range res.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if res.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&res.Body))
	}
	fmt.Println("==============================")

	return res, err
}

////////////////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////////////////

type BucketTest struct {
	ctx    context.Context
	bucket gcs.Bucket
}

var _ SetUpInterface = &BucketTest{}

func (t *BucketTest) SetUp(ti *TestInfo) {
	// Create a context and bucket.
	t.ctx = context.Background()
	t.bucket = getBucketOrDie()

	// Ensure that the bucket is clean.
	deleteAllObjectsOrDie(t.ctx, t.bucket)
}

func (t *BucketTest) createObject(name string, contents string) error {
	return createObject(
		t.ctx,
		t.bucket,
		&storage.ObjectAttrs{Name: name},
		contents)
}

func (t *BucketTest) readObject(objectName string) (contents string, err error) {
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

type CreateTest struct {
	BucketTest
}

func init() { RegisterTestSuite(&CreateTest{}) }

func (t *CreateTest) EmptyObject() {
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

func (t *CreateTest) NonEmptyObject() {
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

func (t *CreateTest) Overwrite() {
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

func (t *CreateTest) ObjectAttributes_Default() {
	// Create an object with default attributes.
	AssertEq(nil, t.createObject("foo", "taco"))

	// Check what shows up in the listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	AssertEq(1, len(objects.Results))
	o := objects.Results[0]

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
}

func (t *CreateTest) ObjectAttributes_Explicit() {
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

	AssertEq(nil, createObject(t.ctx, t.bucket, attrs, "taco"))

	// Check what shows up in the listing.
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertThat(objects.Prefixes, ElementsAre())
	AssertEq(nil, objects.Next)

	AssertEq(1, len(objects.Results))
	o := objects.Results[0]

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
}

func (t *CreateTest) WriteThenAbandon() {
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

func (t *CreateTest) InterestingNames() {
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
		"foo\u000bbar",
		"foo\u000cbar",
		"foo\u0085bar",
		"foo\u2028bar",
		"foo\u2029bar",
	}

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

func (t *CreateTest) IllegalNames() {
	// Naming requirements:
	// Cf. https://cloud.google.com/storage/docs/bucket-naming
	const maxLegalLength = 1024

	names := []string{
		// Empty and too long
		"",
		strings.Repeat("a", maxLegalLength+1),

		// Carriage return and line feed
		"foo\u000abar",
		"foo\u000dbar",
	}

	// Make sure we cannot create any of the names above.
	for _, name := range names {
		nameDump := hex.Dump([]byte(name))

		err := t.createObject(name, "")
		AssertNe(nil, err, nameDump)

		if name == "" {
			ExpectThat(err, Error(HasSubstr("Required")), nameDump)
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

type ReadTest struct {
	BucketTest
}

func init() { RegisterTestSuite(&ReadTest{}) }

func (t *ReadTest) NonExistentObject() {
	_, err := t.bucket.NewReader(t.ctx, "foobar")

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("object doesn't exist")))
}

func (t *ReadTest) EmptyObject() {
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

func (t *ReadTest) NonEmptyObject() {
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

type DeleteTest struct {
	BucketTest
}

func init() { RegisterTestSuite(&DeleteTest{}) }

func (t *DeleteTest) NonExistentObject() {
	err := t.bucket.DeleteObject(t.ctx, "foobar")

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("Not Found")))
}

func (t *DeleteTest) Successful() {
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

type ListTest struct {
	BucketTest
}

func init() { RegisterTestSuite(&ListTest{}) }

func (t *ListTest) EmptyBucket() {
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	ExpectThat(objects.Results, ElementsAre())
	ExpectThat(objects.Prefixes, ElementsAre())
	ExpectEq(nil, objects.Next)
}

func (t *ListTest) NewlyCreatedObject() {
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
	ExpectEq("application/octet-stream", o.ContentType)
	ExpectEq("", o.ContentLanguage)
	ExpectEq("", o.CacheControl)
	ExpectEq(len("taco"), o.Size)
}

func (t *ListTest) TrivialQuery() {
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

func (t *ListTest) Delimiter() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
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
	ExpectThat(objects.Prefixes, ElementsAre("b!", "c!", "d!"))

	// Objects
	AssertEq(3, len(objects.Results))

	ExpectEq("a", objects.Results[0].Name)
	ExpectEq("b", objects.Results[1].Name)
	ExpectEq("e", objects.Results[2].Name)
}

func (t *ListTest) Prefix() {
	// Create several objects.
	AssertEq(
		nil,
		createEmpty(
			t.ctx,
			t.bucket,
			[]string{
				"a",
				"a\xff",
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

func (t *ListTest) DelimiterAndPrefix() {
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
				"blah!a\xff",
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

func (t *ListTest) Cursor() {
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
			"h"))

	ExpectThat(
		prefixes,
		ElementsAre(
			"c!",
			"d!",
			"e!",
			"f!",
			"g!"))
}
