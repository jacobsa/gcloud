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

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcscaching"
	. "github.com/jacobsa/ogletest"
)

func TestStatCache(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Invariant-checking cache
////////////////////////////////////////////////////////////////////////

type invariantsCache struct {
	wrapped gcscaching.StatCache
}

func (c *invariantsCache) Insert(
	o *gcs.Object,
	expiration time.Time) {
	c.wrapped.CheckInvariants()
	defer c.wrapped.CheckInvariants()

	c.wrapped.Insert(o, expiration)
	return
}

func (c *invariantsCache) Erase(name string) {
	c.wrapped.CheckInvariants()
	defer c.wrapped.CheckInvariants()

	c.wrapped.Erase(name)
	return
}

func (c *invariantsCache) LookUp(name string, now time.Time) (o *gcs.Object) {
	c.wrapped.CheckInvariants()
	defer c.wrapped.CheckInvariants()

	o = c.wrapped.LookUp(name, now)
	return
}

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

var someTime = time.Date(2015, 4, 5, 2, 15, 0, 0, time.Local)

type StatCacheTest struct {
	cache invariantsCache
}

func init() { RegisterTestSuite(&StatCacheTest{}) }

func (t *StatCacheTest) SetUp(ti *TestInfo) {
	const capacity = 100
	t.cache.wrapped = gcscaching.NewStatCache(capacity)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *StatCacheTest) LookUpInEmptyCache() {
	AssertFalse(true, "TODO")
}

func (t *StatCacheTest) LookUpUnknownKey() {
	AssertFalse(true, "TODO")
}

func (t *StatCacheTest) FillUpToCapacity() {
	AssertFalse(true, "TODO")
}

func (t *StatCacheTest) ExpiresLeastRecentlyUsed() {
	AssertFalse(true, "TODO")
}

func (t *StatCacheTest) Overwrite() {
	AssertFalse(true, "TODO")
}
