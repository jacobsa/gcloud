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
	"testing"

	"github.com/jacobsa/gcloud/gcs"
	. "github.com/jacobsa/ogletest"
)

func TestOgletest(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// Return a bucket based on the contents of command-line flags, exiting the
// process if misconfigured.
func getBucketOrDie() gcs.Bucket

// Delete everything in the bucket, exiting the process on failure.
func deleteAllObjectsOrDie(b gcs.Bucket)

////////////////////////////////////////////////////////////////////////
// Listing
////////////////////////////////////////////////////////////////////////

type ListingTest struct {
	bucket gcs.Bucket
}

func init() { RegisterTestSuite(&ListingTest{}) }

func (t *ListingTest) SetUp() {
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
