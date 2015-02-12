// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import "github.com/jacobsa/gcloud/gcs"

// Create an in-memory bucket with the given name and empty contents.
func NewFakeBucket(name string) gcs.Bucket
