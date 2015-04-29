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

package gcscaching_test

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcscaching"
	"github.com/jacobsa/gcloud/gcs/gcsfake"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestIntegration(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type IntegrationTest struct {
	ctx context.Context

	cache   gcscaching.StatCache
	clock   timeutil.SimulatedClock
	wrapped gcs.Bucket

	bucket gcs.Bucket
}

func init() { RegisterTestSuite(&IntegrationTest{}) }

func (t *IntegrationTest) SetUp(ti *TestInfo) {
	t.ctx = context.Background()

	// Set up a fixed, non-zero time.
	t.clock.SetTime(time.Date(2015, 4, 5, 2, 15, 0, 0, time.Local))

	// Set up dependencies.
	const cacheCapacity = 100
	t.cache = gcscaching.NewStatCache(cacheCapacity)
	t.wrapped = gcsfake.NewFakeBucket(&t.clock, "some_bucket")

	t.bucket = gcscaching.NewFastStatBucket(
		ttl,
		t.cache,
		&t.clock,
		t.wrapped)
}

func (t *IntegrationTest) stat(name string) (o *gcs.Object, err error) {
	req := &gcs.StatObjectRequest{
		Name: name,
	}

	o, err = t.bucket.StatObject(t.ctx, req)
	return
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *IntegrationTest) StatDoesntCacheNotFoundErrors() {
	const name = "taco"
	var err error

	// Stat an unknown object.
	_, err = t.stat(name)
	AssertThat(err, HasSameTypeAs(&gcs.NotFoundError{}))

	// Create the object through the back door.
	_, err = gcsutil.CreateObject(t.ctx, t.wrapped, name, "")
	AssertEq(nil, err)

	// Stat again. We should now see the object.
	o, err := t.stat(name)
	AssertEq(nil, err)
	ExpectNe(nil, o)
}

func (t *IntegrationTest) CreateInsertsIntoCache() {
	const name = "taco"
	var err error

	// Create an object.
	_, err = gcsutil.CreateObject(t.ctx, t.bucket, name, "")
	AssertEq(nil, err)

	// Delete it through the back door.
	err = t.wrapped.DeleteObject(t.ctx, name)
	AssertEq(nil, err)

	// StatObject should still see it.
	o, err := t.stat(name)
	AssertEq(nil, err)
	ExpectNe(nil, o)
}

func (t *IntegrationTest) StatInsertsIntoCache() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) ListInsertsIntoCache() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) UpdateUpdatesCache() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) DeleteRemovesFromCache() {
	AssertFalse(true, "TODO")
}

func (t *IntegrationTest) Expiration() {
	AssertFalse(true, "TODO")
}
