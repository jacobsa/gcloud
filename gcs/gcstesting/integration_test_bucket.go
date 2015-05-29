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

package gcstesting

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/jacobsa/gcloud/gcs"
)

var fBucket = flag.String(
	"bucket", "",
	"Bucket to use for testing.")

// Return an HTTP client configured to use application default credentials
// (https://goo.gl/ZAhqjq).
//
// For use in integration tests that use GCS.
func IntegrationTestHTTPClient() (client *http.Client, err error) {
	const scope = gcs.Scope_FullControl
	client, err = google.DefaultClient(context.Background(), scope)
	return
}

func bucketName() (name string, err error) {
	name = *fBucket
	if name == "" {
		err = errors.New("You must set --bucket.")
		return
	}

	return
}

// Return a bucket configured according to the --bucket flag defined by this
// package, using application default credentials (https://goo.gl/ZAhqjq).
//
// For use in integration tests that use GCS.
func IntegrationTestBucket() (b gcs.Bucket, err error) {
	// Grab the bucket name.
	name, err := bucketName()
	if err != nil {
		return
	}

	// Grab the HTTP client.
	client, err := IntegrationTestHTTPClient()
	if err != nil {
		return
	}

	// Set up a GCS connection.
	cfg := &gcs.ConnConfig{
		HTTPClient:      client,
		MaxBackoffSleep: 5 * time.Minute,
	}

	conn, err := gcs.NewConn(cfg)
	if err != nil {
		err = fmt.Errorf("gcs.NewConn: %v", err)
		return
	}

	// Open the bucket.
	b = conn.GetBucket(name)

	return
}

// Like IntegrationTestBucket, but exits the process on failure.
func IntegrationTestBucketOrDie() (b gcs.Bucket) {
	b, err := IntegrationTestBucket()
	if err != nil {
		log.Fatalln("IntegrationTestBucket:", err)
	}

	return
}
