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

package gcs

import (
	"io/ioutil"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"golang.org/x/net/context"

	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestRetry(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type retryBucketTest struct {
	ctx     context.Context
	wrapped MockBucket
	bucket  Bucket
}

func (t *retryBucketTest) SetUp(ti *TestInfo) {
	t.ctx = ti.Ctx
	t.wrapped = NewMockBucket(ti.MockController, "wrapped")
	t.bucket = newRetryBucket(time.Second, t.wrapped)
}

////////////////////////////////////////////////////////////////////////
// CreateObject
////////////////////////////////////////////////////////////////////////

type RetryBucket_CreateObjectTest struct {
	retryBucketTest

	req CreateObjectRequest
	obj *Object
}

func init() { RegisterTestSuite(&RetryBucket_CreateObjectTest{}) }

func (t *RetryBucket_CreateObjectTest) call() (err error) {
	t.obj, err = t.bucket.CreateObject(t.ctx, &t.req)
	return
}

func (t *RetryBucket_CreateObjectTest) ErrorReading() {
	var err error

	// Pass in a reader that will return an error.
	t.req.Contents = ioutil.NopCloser(
		iotest.OneByteReader(
			iotest.TimeoutReader(
				strings.NewReader("foobar"))))

	// Call
	err = t.call()

	ExpectThat(err, Error(HasSubstr("ReadAll")))
	ExpectThat(err, Error(HasSubstr("timeout")))
}

func (t *RetryBucket_CreateObjectTest) Successful() {
	AssertTrue(false, "TODO")
}

func (t *RetryBucket_CreateObjectTest) ShouldNotRetry() {
	AssertTrue(false, "TODO")
}

func (t *RetryBucket_CreateObjectTest) RetrySuccessful() {
	AssertTrue(false, "TODO")
}
