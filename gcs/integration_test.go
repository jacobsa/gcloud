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

type IntegrationTest struct{}

func init() { RegisterTestSuite(&IntegrationTest{}) }

func (t *IntegrationTest) DoesFoo() {
	AssertFalse(true, "TODO")
}
