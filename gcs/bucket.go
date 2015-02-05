// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

// Bucket represents a GCS bucket, pre-bound with a bucket name and necessary
// authorization information.
//
// Each method that may block accepts a context object that is used for
// deadlines and cancellation. Users need not package authorization information
// into the context object (using cloud.WithContext or similar).
type Bucket interface {
	Name() string

	// List the objects in the bucket that meet the criteria defined by the
	// query, returning a result object that contains the results and potentially
	// a cursor for retrieving the next portion of the larger set of results.
	ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error)
}

type bucket struct {
	projID string
	client *http.Client
	name   string
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.ListObjects(authContext, b.name, query)
}
