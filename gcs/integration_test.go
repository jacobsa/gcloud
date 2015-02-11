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
	"flag"
	"io"
	"log"
	"net/http"
	"testing"

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

func createWithContents(ctx context.Context, bucket gcs.Bucket, name string, contents string) error {
	// Create a writer.
	attrs := &storage.ObjectAttrs{
		Name: name,
	}

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
				if err := createWithContents(ctx, bucket, objectName, ""); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return bundle.Join()
}

////////////////////////////////////////////////////////////////////////
// Listing
////////////////////////////////////////////////////////////////////////

type ListingTest struct {
	ctx    context.Context
	bucket gcs.Bucket
}

var _ SetUpInterface = &ListingTest{}

func init() { RegisterTestSuite(&ListingTest{}) }

func (t *ListingTest) SetUp(ti *TestInfo) {
	// Create a context and bucket.
	t.ctx = context.Background()
	t.bucket = getBucketOrDie()

	// Ensure that the bucket is clean.
	deleteAllObjectsOrDie(t.ctx, t.bucket)
}

func (t *ListingTest) createObject(name string, contents string) error {
	// Create a writer.
	attrs := &storage.ObjectAttrs{
		Name: name,
	}

	writer, err := t.bucket.NewWriter(t.ctx, attrs)
	if err != nil {
		return err
	}

	// Copy into the writer.
	_, err = io.Copy(writer, bytes.NewReader([]byte(contents)))

	// Close the writer.
	return writer.Close()
}

/////////////////////////
// Test functions
/////////////////////////

func (t *ListingTest) EmptyBucket() {
	objects, err := t.bucket.ListObjects(t.ctx, nil)
	AssertEq(nil, err)

	AssertNe(nil, objects)
	ExpectThat(objects.Results, ElementsAre())
	ExpectThat(objects.Prefixes, ElementsAre())
	ExpectEq(nil, objects.Next)
}

func (t *ListingTest) NewlyCreatedObject() {
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

func (t *ListingTest) TrivialQuery() {
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

func (t *ListingTest) Delimiter() {
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

func (t *ListingTest) Prefix() {
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

func (t *ListingTest) DelimiterAndPrefix() {
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

func (t *ListingTest) Cursor() {
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

func (t *ListingTest) Ordering() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Atomicity() {
	AssertFalse(true, "TODO")
}

// TODO(jacobsa): CreateTest.Korean
// TODO(jacobsa): CreateTest.CarriageReturn
// TODO(jacobsa): CreateTest.LineFeed
// TODO(jacobsa): CreateTest.FormFeed
