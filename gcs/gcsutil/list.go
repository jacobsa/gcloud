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
	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Repeatedly call bucket.ListObjects until there is nothing further to list,
// returning all objects and prefixes encountered.
func List(
	ctx context.Context,
	bucket gcs.Bucket,
	q *storage.Query) (objects []*storage.Object, prefixes []string, err error) {
	for q != nil {
		// Grab one set of results.
		var listing *storage.Objects
		if listing, err = bucket.ListObjects(ctx, q); err != nil {
			return
		}

		// Accumulate the results.
		objects = append(objects, listing.Results...)
		prefixes = append(prefixes, listing.Prefixes...)

		// Move on to the next query, if necessary.
		q = listing.Next
	}

	return
}
