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
	"io"
	"time"

	"golang.org/x/net/context"

	"github.com/googlecloudplatform/gcsfuse/timeutil"
	"github.com/jacobsa/gcloud/syncutil"
)

// Create a bucket that caches object records returned by the supplied wrapped
// bucket. Records are invalidated when modifications are made through this
// bucket, and after the supplied TTL.
func NewFastStatBucket(
	ttl time.Duration,
	cache StatCache,
	clock timeutil.Clock,
	wrapped Bucket) (b Bucket) {
	fsb := &fastStatBucket{
		cache:   cache,
		clock:   clock,
		wrapped: wrapped,
		ttl:     ttl,
	}

	fsb.mu = syncutil.NewInvariantMutex(fsb.checkInvariants)

	b = fsb
	return
}

type fastStatBucket struct {
	mu syncutil.InvariantMutex

	/////////////////////////
	// Dependencies
	/////////////////////////

	// GUARDED_BY(mu)
	cache StatCache

	clock   timeutil.Clock
	wrapped Bucket

	/////////////////////////
	// Constant data
	/////////////////////////

	ttl time.Duration
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// LOCKS_REQUIRED(b.mu)
func (b *fastStatBucket) checkInvariants()

////////////////////////////////////////////////////////////////////////
// Bucket interface
////////////////////////////////////////////////////////////////////////

func (b *fastStatBucket) Name() string

func (b *fastStatBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error)

// LOCKS_EXCLUDED(b.mu)
func (b *fastStatBucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error)

// LOCKS_EXCLUDED(b.mu)
func (b *fastStatBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error)

// LOCKS_EXCLUDED(b.mu)
func (b *fastStatBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error)

// LOCKS_EXCLUDED(b.mu)
func (b *fastStatBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error)

// LOCKS_EXCLUDED(b.mu)
func (b *fastStatBucket) DeleteObject(
	ctx context.Context,
	name string) (err error)
