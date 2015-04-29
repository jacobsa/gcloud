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

// Restrict this (slow) test to builds that specify the tag 'integration'.
// +build integration

package gcscaching_test

import (
	"log"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcscaching"
	"github.com/jacobsa/gcloud/gcs/gcstesting"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestIntegration(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type IntegrationTest struct {
	cache   gcscaching.StatCache
	clock   timeutil.SimulatedClock
	wrapped gcs.Bucket

	bucket gcs.Bucket
}

func init() { RegisterTestSuite(&IntegrationTest{}) }

func (t *IntegrationTest) SetUp(ti *TestInfo) {
	// Grab the underlying bucket and empty it.
	wrapped := gcstesting.IntegrationTestBucketOrDie()
	err := gcsutil.DeleteAllObjects(context.Background(), wrapped)
	if err != nil {
		log.Fatalln("DeleteAllObjects:", err)
	}

	// Set up a fixed, non-zero time.
	t.clock.SetTime(time.Date(2015, 4, 5, 2, 15, 0, 0, time.Local))

	// Set up dependencies.
	const cacheCapacity = 100
	t.cache = gcscaching.NewStatCache(cacheCapacity)
	t.wrapped = wrapped

	t.bucket = gcscaching.NewFastStatBucket(
		ttl,
		t.cache,
		&t.clock,
		t.wrapped)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *IntegrationTest) StatUnknownTwice() {
	var err error
	req := &gcs.StatObjectRequest{
		Name: "taco",
	}

	// First
	_, err = t.bucket.StatObject(context.Background(), req)
	ExpectThat(err, HasSameTypeAs(&gcs.NotFoundError{}))

	// Second
	_, err = t.bucket.StatObject(context.Background(), req)
	ExpectThat(err, HasSameTypeAs(&gcs.NotFoundError{}))
}

func (t *IntegrationTest) CreateThenStat() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) ListThenStat() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) CreateThenUpdateThenStat() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) DeleteThenStat() {
	AssertFalse(true, "TODO")
}
