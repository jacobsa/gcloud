// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// An integration test that uses the real GCS. Run it with appropriate flags as
// follows:
//
//     go test -v -tags integration . -bucket <bucket name>
//
// The bucket's contents are not preserved.
//
// The first time you run the test, it may die with a URL to visit to obtain an
// authorization code after authorizing the test to access your bucket. Run it
// again with the "-oauthutil.auth_code" flag afterward.

// Restrict this (slow) test to builds that specify the tag 'integration'.
// +build integration

package gcs_test

import (
	"flag"
	"log"
	"net/http"
	"testing"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcstesting"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	"github.com/jacobsa/gcloud/oauthutil"
	"github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	storagev1 "google.golang.org/api/storage/v1"
)

////////////////////////////////////////////////////////////////////////
// Wiring code
////////////////////////////////////////////////////////////////////////

var fBucket = flag.String("bucket", "", "Empty bucket to use for storage.")

func getHttpClientOrDie() *http.Client {
	// Set up a token source.
	config := &oauth2.Config{
		ClientID:     "517659276674-k9tr62f5rpd1k6ivvhadq0etbu4gu3t5.apps.googleusercontent.com",
		ClientSecret: "A6Xo63GDMRHmZ2TB7CO99lLN",
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
		Scopes:       []string{storagev1.DevstorageRead_writeScope},
		Endpoint:     google.Endpoint,
	}

	const cacheFileName = ".gcs_integration_test.token_cache.json"
	httpClient, err := oauthutil.NewTerribleHttpClient(config, cacheFileName)
	if err != nil {
		panic("NewTerribleHttpClient: " + err.Error())
	}

	return httpClient
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
// Registration
////////////////////////////////////////////////////////////////////////

func TestOgletest(t *testing.T) { ogletest.RunTests(t) }

func init() {
	gcstesting.RegisterBucketTests(func() gcs.Bucket {
		bucket := getBucketOrDie()

		if err := gcsutil.DeleteAllObjects(context.Background(), bucket); err != nil {
			panic("DeleteAllObjects: " + err.Error())
		}

		return bucket
	})
}
