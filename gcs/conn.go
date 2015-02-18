// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"fmt"
	"net/http"

	storagev1 "google.golang.org/api/storage/v1"
)

// Conn represents a connection to GCS, pre-bound with a project ID and
// information required for authorization.
type Conn interface {
	// Return a Bucket object representing the GCS bucket with the given name. No
	// immediate validation is performed.
	GetBucket(name string) Bucket
}

type conn struct {
	projID     string
	client     *http.Client
	rawService *storagev1.Service
}

// Open a connection to GCS for the project with the given ID using the
// supplied HTTP client, which is assumed to handle authorization and
// authentication.
func NewConn(projID string, client *http.Client) (c Conn, err error) {
	impl := &conn{
		projID: projID,
		client: client,
	}

	c = impl

	// Create a raw service for the storagev1 package.
	if impl.rawService, err = storagev1.New(impl.client); err != nil {
		err = fmt.Errorf("storagev1.New: %v", err)
		return
	}

	return
}

func (c *conn) GetBucket(name string) Bucket {
	return newBucket(c.projID, c.client, c.rawService, name)
}
