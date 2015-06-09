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
	"errors"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	"github.com/jacobsa/gcloud/httputil"
	"github.com/jacobsa/reqtrace"

	storagev1 "google.golang.org/api/storage/v1"
)

// OAuth scopes for GCS. For use with e.g. google.DefaultTokenSource.
const (
	Scope_FullControl = storagev1.DevstorageFullControlScope
	Scope_ReadOnly    = storagev1.DevstorageReadOnlyScope
	Scope_ReadWrite   = storagev1.DevstorageReadWriteScope
)

// Conn represents a connection to GCS, pre-bound with a project ID and
// information required for authorization.
type Conn interface {
	// Return a Bucket object representing the GCS bucket with the given name.
	// Attempt to fail early in the case of bad credentials.
	OpenBucket(name string) (b Bucket, err error)
}

// Configuration accepted by NewConn.
type ConnConfig struct {
	// An oauth2 token source to use for authenticating to GCS.
	//
	// You probably want this one:
	//     http://godoc.org/golang.org/x/oauth2/google#DefaultTokenSource
	TokenSource oauth2.TokenSource

	// The value to set in User-Agent headers for outgoing HTTP requests. If
	// empty, a default will be used.
	UserAgent string

	// The maximum amount of time to spend sleeping in a retry loop with
	// exponential backoff for failed requests. The default of zero disables
	// automatic retries.
	//
	// If you enable automatic retries, beware of the following:
	//
	//  *  Bucket.CreateObject will buffer the entire object contents in memory,
	//     so your object contents must not be too large to fit.
	//
	//  *  Bucket.NewReader needs to perform an additional round trip to GCS in
	//     order to find the latest object generation if you don't specify a
	//     particular generation.
	//
	//  *  Make sure your operations are idempotent, or that your application can
	//     tolerate it if not.
	//
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

	// Set up the HTTP transport, enabling debugging if requested.
	if cfg.TokenSource == nil {
		err = errors.New("You must set TokenSource.")
		return
	}

	var transport httputil.CancellableRoundTripper = &oauth2.Transport{
		Source: cfg.TokenSource,
		Base:   http.DefaultTransport,
	}

	transport = httputil.DebuggingRoundTripper(transport)

	// Set up the connection.
	c = &conn{
		client:          &http.Client{Transport: transport},
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

func (c *conn) OpenBucket(name string) (b Bucket, err error) {
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

	// Print debug output when enabled.
	b = newDebugBucket(b)

	// TODO(jacobsa): Check credentials. See here for more:
	// https://github.com/GoogleCloudPlatform/gcsfuse/issues/65

	return
}
