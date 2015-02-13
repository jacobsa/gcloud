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
// again with the "-auth_code" flag afterward.

// Restrict this (slow) test to builds that specify the tag 'integration'.
// +build integration

package gcs_test

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
var fAuthCode = flag.String("auth_code", "", "Auth code from GCS console.")
var fDebugHttp = flag.Bool("debug_http", false, "Dump information about HTTP requests.")

func getHttpClientOrDie() *http.Client {
	// Set up a token source.
	config := &oauth2.Config{
		ClientID:     "517659276674-k9tr62f5rpd1k6ivvhadq0etbu4gu3t5.apps.googleusercontent.com",
		ClientSecret: "A6Xo63GDMRHmZ2TB7CO99lLN",
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
		Scopes:       []string{storagev1.DevstorageRead_writeScope},
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
