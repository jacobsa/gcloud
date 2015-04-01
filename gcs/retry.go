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
)

// A bucket that wraps another, calling its methods in a retry loop with
// randomized exponential backoff.
type retryBucket struct {
	maxSleep time.Duration
	wrapped  Bucket
}

var _ Bucket = &retryBucket{}

func (rb *retryBucket) Name() (name string) {
	name = rb.wrapped.Name()
	return
}

func (rb *retryBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error) {
	rc, err = rb.wrapped.NewReader(ctx, req)
	return
}

func (rb *retryBucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error) {
	o, err = rb.wrapped.CreateObject(ctx, req)
	return
}

func (rb *retryBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	o, err = rb.wrapped.StatObject(ctx, req)
	return
}

func (rb *retryBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	listing, err = rb.wrapped.ListObjects(ctx, req)
	return
}

func (rb *retryBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	o, err = rb.wrapped.UpdateObject(ctx, req)
	return
}

func (rb *retryBucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	err = rb.wrapped.DeleteObject(ctx, name)
	return
}
