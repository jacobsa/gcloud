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
	"io"

	"github.com/jacobsa/gcloud/reqtrace"

	"golang.org/x/net/context"
)

// A bucket that uses reqtrace.Trace to annotate calls.
type reqtraceBucket struct {
	Wrapped Bucket
}

////////////////////////////////////////////////////////////////////////
// Bucket interface
////////////////////////////////////////////////////////////////////////

func (b *reqtraceBucket) Name() string {
	return b.Wrapped.Name()
}

func (b *reqtraceBucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error) {
	// TODO(jacobsa): Do something useful for this method. Probably a bespoke
	// ReadCloser whose close method reports any errors seen while reading. What
	// to do if it's never closed? Maybe watch ctx.Done()? That's still not
	// guaranteed to be non-nil. I guess we could just fail to trace in that
	// case.
	rc, err = b.Wrapped.NewReader(ctx, req)
	return
}

func (b *reqtraceBucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error) {
	desc := fmt.Sprintf("CreateObject: %s", sanitizeObjectName(req.Name))
	defer reqtrace.StartSpanWithError(&ctx, &err, desc)()

	o, err = b.Wrapped.CreateObject(ctx, req)
	return
}

func (b *reqtraceBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	desc := fmt.Sprintf("StatObject: %s", sanitizeObjectName(req.Name))
	defer reqtrace.StartSpanWithError(&ctx, &err, desc)()

	o, err = b.Wrapped.StatObject(ctx, req)
	return
}

func (b *reqtraceBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	desc := fmt.Sprintf("ListObjects")
	defer reqtrace.StartSpanWithError(&ctx, &err, desc)()

	listing, err = b.Wrapped.ListObjects(ctx, req)
	return
}

func (b *reqtraceBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	desc := fmt.Sprintf("UpdateObject: %s", sanitizeObjectName(req.Name))
	defer reqtrace.StartSpanWithError(&ctx, &err, desc)()

	o, err = b.Wrapped.UpdateObject(ctx, req)
	return
}

func (b *reqtraceBucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	desc := fmt.Sprintf("DeleteObject: %s", sanitizeObjectName(name))
	defer reqtrace.StartSpanWithError(&ctx, &err, desc)()

	err = b.Wrapped.DeleteObject(ctx, name)
	return
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func sanitizeObjectName(
	name string) (sanitized string) {
	sanitized = fmt.Sprintf("%q", name)
	return
}
