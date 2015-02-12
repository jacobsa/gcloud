// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcstesting

import "github.com/jacobsa/gcloud/gcs"

// An interface that all bucket tests must implement.
type bucketTestSetUpInterface interface {
	SetUpBucketTest(b gcs.Bucket)
}

// Given a function that returns an initialized, empty bucket, register test
// suites that exercise the buckets returned by the function with ogletest.
func RegisterBucketTests(func() gcs.Bucket) {
	// A list of empty instances of the test suites we want to register.
	suitePrototypes := []bucketTestSetUpInterface{
		&createTest{},
		&readTest{},
		&deleteTest{},
		&listTest{},
	}
}
