// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

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
