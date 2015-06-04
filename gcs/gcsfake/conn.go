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

package gcsfake

import (
	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/syncutil"
)

// Create an "in-memory GCS" that allows access to buckets of any name, each
// initially with empty contents. The supplied clock will be used for
// generating timestamps.
func NewConn(clock timeutil.Clock) (c gcs.Conn) {
	typed := &conn{
		buckets: make(map[string]gcs.Bucket),
	}

	typed.mu = syncutil.NewInvariantMutex(typed.checkInvariants)

	c = typed
	return
}

////////////////////////////////////////////////////////////////////////
// Implementation
////////////////////////////////////////////////////////////////////////

type conn struct {
	mu syncutil.InvariantMutex

	// INVARIANT: For each k, v: v.Name() == k
	//
	// GUARDED_BY(mu)
	buckets map[string]gcs.Bucket
}

func (c *conn) checkInvariants() {
	panic("TODO")
}

func (c *conn) GetBucket(name string) (b gcs.Bucket) {
	panic("TODO")
}
