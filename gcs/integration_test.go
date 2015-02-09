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

	. "github.com/jacobsa/ogletest"
)

func TestIntegration(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Listing
////////////////////////////////////////////////////////////////////////

type ListingTest struct{}

func init() { RegisterTestSuite(&ListingTest{}) }

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
