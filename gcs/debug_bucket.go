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
	"flag"
	"io"
	"log"

	"golang.org/x/net/context"
)

var fDebug = flag.Bool(
	"gcs.debug",
	false,
	"Write FUSE debugging messages to stderr.")

// If debugging is enabled, wrap the supplied bucket in a layer that prints
// debug messages.
func newDebugBucket(wrapped Bucket) (b Bucket) {
	// TODO
	b = wrapped
	return
}

type debugBucket struct {
	logger  log.Logger
	wrapped Bucket
}

////////////////////////////////////////////////////////////////////////
// Bucket interface
////////////////////////////////////////////////////////////////////////

func (b *debugBucket) Name() string {
	return b.Wrapped.Name()
}

func (b *debugBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error) {
	err = errors.New("TODO: NewReader")
	return
}

func (b *debugBucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error) {
	err = errors.New("TODO")
	return
}

func (b *debugBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	err = errors.New("TODO")
	return
}

func (b *debugBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	err = errors.New("TODO")
	return
}

func (b *debugBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	err = errors.New("TODO")
	return
}

func (b *debugBucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	err = errors.New("TODO")
	return
}
