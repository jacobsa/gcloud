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

package gcs

import (
	"fmt"
	"net/http"

	storagev1 "google.golang.org/api/storage/v1"
)

// OAuth scopes for GCS. For use with e.g. oauthutil.NewJWTHttpClient.
const (
	Scope_FullControl = storagev1.DevstorageFull_controlScope
	Scope_ReadOnly    = storagev1.DevstorageRead_onlyScope
	Scope_ReadWrite   = storagev1.DevstorageRead_writeScope
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
