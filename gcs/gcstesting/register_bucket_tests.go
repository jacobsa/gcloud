// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcstesting

import "github.com/jacobsa/gcloud/gcs"

// Given a function that returns an initialized, empty bucket, register test
// suites that exercise the buckets returned by the function with ogletest.
func RegisterBucketTests(func() gcs.Bucket)
