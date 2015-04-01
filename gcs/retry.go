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

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
)

// A bucket that wraps another, calling its methods in a retry loop with
// randomized exponential backoff.
type retryBucket struct {
	maxSleep time.Duration
	wrapped  gcs.Bucket
}

func (rb *retryBucket) Name() string

func (rb *retryBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (io.ReadCloser, error)

func (rb *retryBucket) reateObject(
	ctx context.Context,
	req *CreateObjectRequest) (*Object, error)

func (rb *retryBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (*Object, error)

func (rb *retryBucket) istObjects(
	ctx context.Context,
	req *ListObjectsRequest) (*Listing, error)

func (rb *retryBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (*Object, error)

func (rb *retryBucket) eleteObject(ctx context.Context, name string) error
