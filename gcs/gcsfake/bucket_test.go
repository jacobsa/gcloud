// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake_test

import (
	"testing"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcsfake"
	"github.com/jacobsa/gcloud/gcs/gcstesting"
	"github.com/jacobsa/ogletest"
)

func TestOgletest(t *testing.T) { ogletest.RunTests(t) }

func init() {
	gcstesting.RegisterBucketTests(func() gcs.Bucket {
		return gcsfake.NewFakeBucket("some_bucket")
	})
}
