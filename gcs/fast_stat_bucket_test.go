// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcs_test

import (
	"testing"
	"time"

	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/gcs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestFastStatBucket(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

const ttl = time.Second

type FastStatBucketTest struct {
	cache   mock_gcs.MockStatCache
	clock   timeutil.SimulatedClock
	wrapped mock_gcs.MockBucket

	bucket gcs.Bucket
}

func init() { RegisterTestSuite(&FastStatBucketTest{}) }

func (t *FastStatBucketTest) SetUp(ti *TestInfo) {
	panic("TODO")
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *FastStatBucketTest) DoesFoo() {
	AssertFalse(true, "TODO")
}
