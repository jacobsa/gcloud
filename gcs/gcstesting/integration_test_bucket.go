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

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/oauthutil"
	storagev1 "google.golang.org/api/storage/v1"
)

var fKeyFile = flag.String(
	"key_file", "",
	"Path to a JSON key for a service account created on the Developers Console.")

var fBucket = flag.String(
	"bucket", "",
	"Empty bucket to use for storage.")

func httpClient() (client *http.Client, err error) {
	if *fKeyFile == "" {
		err = errors.New("You must set --key_file.")
		return
	}

	const scope = storagev1.DevstorageFull_controlScope
	client, err = oauthutil.NewJWTHttpClient(*fKeyFile, []string{scope})
	if err != nil {
		err = fmt.Errorf("oauthutil.NewJWTHttpClient: %v", err)
		return
	}

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

// Return a bucket configured according to the --bucket and --key_file flags
// defined by this package. For use in integration tests that use GCS.
func IntegrationTestBucket() (b gcs.Bucket, err error) {
	// Grab the bucket name.
	name, err := bucketName()
	if err != nil {
		return
	}

	// Grab the HTTP client.
	client, err := httpClient()
	if err != nil {
		return
	}

	// Set up a GCS connection.
	cfg := &gcs.ConnConfig{
		HTTPClient: client,
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
