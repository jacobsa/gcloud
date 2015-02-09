// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// An integration test that uses the real GCS. Run it with appropriate flags as
// follows:
//
//     TODO(jacobsa): Invocation example.
//
// The bucket must be empty initially. The test will attempt to clean up after
// itself, but no guarantees.

package gcs_test

import (
	"flag"
	"log"
	"net/http"
	"testing"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/oauthutil"
	. "github.com/jacobsa/ogletest"
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
		ClientID:     "517659276674-6am1g9cau0bc0pbre6d59d9b5168b1tr.apps.googleusercontent.com",
		ClientSecret: "YXtgLuEVJoTlG-3D-4UoGEGq",
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

// Delete everything in the bucket, exiting the process on failure.
func deleteAllObjectsOrDie(b gcs.Bucket) {
	log.Fatalln("TODO(jacobsa): Implement deleteAllObjectsOrDie.")
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
	deleteAllObjectsOrDie(t.bucket)
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
