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

package gcsutil

import (
	"strings"

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create an object with the supplied contents in the given bucket. attrs.Name
// must be set; all other fields are optional.
func CreateObject(
	ctx context.Context,
	bucket gcs.Bucket,
	attrs *storage.ObjectAttrs,
	contents string) (*storage.Object, error) {
	req := &gcs.CreateObjectRequest{
		Attrs:    *attrs,
		Contents: strings.NewReader(contents),
	}

	return bucket.CreateObject(ctx, req)
}
