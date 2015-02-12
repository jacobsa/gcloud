// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcstesting

import (
	"reflect"
	"strings"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/ogletest"
)

// An interface that all bucket tests must implement.
type bucketTestSetUpInterface interface {
	SetUpBucketTest(b gcs.Bucket)
}

func getSuiteName(prototype interface{}) string {
	return strings.Title(reflect.TypeOf(prototype).Name())
}

func getTestMethods(suitePrototype interface{}) []reflect.Method

func registerTestSuite(
	makeBucket func() gcs.Bucket,
	prototype bucketTestSetUpInterface) {
	suiteType := reflect.TypeOf(prototype)

	// We don't need anything fancy at the suite level.
	var ts ogletest.TestSuite
	ts.Name = getSuiteName(prototype)

	// For each method, we create a test function.
	for _, method := range getTestMethods(prototype) {
		var tf ogletest.TestFunction

		// Create an instance to be shared among SetUp and the test function itself.
		var instance reflect.Value = reflect.New(suiteType)

		// SetUp should create a bucket and then initialize the suite object,
		// remembering that the suite implements bucketTestSetUpInterface.
		tf.SetUp = func(*ogletest.TestInfo) {
			bucket := makeBucket()
			instance.Interface().(bucketTestSetUpInterface).SetUpBucketTest(bucket)
		}

		// The test function itself should simply invoke the method.
		methodCopy := method
		tf.Run = func() {
			methodCopy.Func.Call([]reflect.Value{instance})
		}

		// Save the test function.
		ts.TestFunctions = append(ts.TestFunctions, tf)
	}

	// Register the suite.
	ogletest.Register(ts)
}

// Given a function that returns an initialized, empty bucket, register test
// suites that exercise the buckets returned by the function with ogletest.
func RegisterBucketTests(makeBucket func() gcs.Bucket) {
	// A list of empty instances of the test suites we want to register.
	suitePrototypes := []bucketTestSetUpInterface{
		&createTest{},
		&readTest{},
		&deleteTest{},
		&listTest{},
	}

	// Register each.
	for _, suitePrototype := range suitePrototypes {
		registerTestSuite(makeBucket, suitePrototype)
	}
}
