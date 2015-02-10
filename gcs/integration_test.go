// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// An integration test that uses the real GCS. Run it with appropriate flags as
// follows:
//
//     go test -bucket <bucket name>
//
// The bucket must be empty initially. The test will attempt to clean up after
// itself, but no guarantees.
//
// The first time you run the test, it may die with a URL to visit to obtain an
// authorization code after authorizing the test to access your bucket. Run it
// again with the "-auth_code" flag afterward.

package gcs_test

import (
	"flag"
	"log"
	"net/http"
	"testing"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/oauthutil"
	"github.com/jacobsa/gcloud/syncutil"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
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
		Scopes:       []string{storage.DevstorageFull_controlScope},
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
	log.Println("Requesting initial OAuth token.")
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
func listIntoChannel(ctx context.Context, b gcs.Bucket, objectNames chan<- string) error

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

////////////////////////////////////////////////////////////////////////
// Listing
////////////////////////////////////////////////////////////////////////

type ListingTest struct {
	bucket gcs.Bucket
}

func init() { RegisterTestSuite(&ListingTest{}) }

func (t *ListingTest) SetUp(ti *TestInfo) {
	t.bucket = getBucketOrDie()
}

func (t *ListingTest) TearDown() {
	deleteAllObjectsOrDie(context.Background(), t.bucket)
}

func (t *ListingTest) EmptyBucket() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) TrivialQuery() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Delimeter() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Prefix() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) DelimeterAndPrefix() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Cursor() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Ordering() {
	AssertFalse(true, "TODO")
}

func (t *ListingTest) Atomicity() {
	AssertFalse(true, "TODO")
}
