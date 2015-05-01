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
	"net/http"
	"time"

	"github.com/jacobsa/reqtrace"

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

// Configuration accepted by NewConn.
type ConnConfig struct {
	// An HTTP client, assumed to handle authorization and authentication. See
	// github.com/jacobsa/gcloud/oauthutil for a convenient way to create one of
	// these.
	HTTPClient *http.Client

	// The value to set in User-Agent headers for outgoing HTTP requests. If
	// empty, a default will be used.
	UserAgent string

	// The maximum amount of time to spend sleeping in a retry loop with
	// exponential backoff for failed requests. The default of zero disables
	// automatic retries.
	//
	// If you enable automatic retries, beware of idempotency issues.
	MaxBackoffSleep time.Duration
}

// Open a connection to GCS.
func NewConn(cfg *ConnConfig) (c Conn, err error) {
	// Fix the user agent if there is none.
	userAgent := cfg.UserAgent
	if userAgent == "" {
		const defaultUserAgent = "github.com-jacobsa-gloud-gcs"
		userAgent = defaultUserAgent
	}

	c = &conn{
		client:          cfg.HTTPClient,
		userAgent:       userAgent,
		maxBackoffSleep: cfg.MaxBackoffSleep,
	}

	return
}

type conn struct {
	client          *http.Client
	userAgent       string
	maxBackoffSleep time.Duration
}

func (c *conn) GetBucket(name string) (b Bucket) {
	b = newBucket(c.client, c.userAgent, name)

	// Enable retry loops if requested.
	if c.maxBackoffSleep > 0 {
		// TODO(jacobsa): Show the retries as distinct spans in the trace.
		b = newRetryBucket(c.maxBackoffSleep, b)
	}

	// Enable tracing if appropriate.
	if reqtrace.Enabled() {
		b = &reqtraceBucket{
			Wrapped: b,
		}
	}

	return
}
