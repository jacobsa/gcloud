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

	"github.com/jacobsa/reqtrace"

	"golang.org/x/net/context"
)

// A bucket that uses reqtrace.Trace to annotate calls.
type reqtraceBucket struct {
	Wrapped Bucket
}

////////////////////////////////////////////////////////////////////////
// Reporting reader
////////////////////////////////////////////////////////////////////////

// An io.ReadCloser that reports the outcome of the read operation represented
// by the wrapped io.ReadCloser once it is known.
type reportingReadCloser struct {
	Wrapped io.ReadCloser
	Report  reqtrace.ReportFunc

	prevErr error
}

func (rc *reportingReadCloser) Read(p []byte) (n int, err error) {
	// Have we already seen an error? Make sure we don't get ourselves into a
	// state where we report twice.
	if rc.prevErr != nil {
		err = fmt.Errorf("Already saw an error: %v", rc.prevErr)
		return
	}

	// Call through.
	n, err = rc.Wrapped.Read(p)
	if err != nil {
		rc.Report(err)
		rc.prevErr = err
		return
	}

	return
}

func (rc *reportingReadCloser) Close() (err error) {
	// Have we already seen an error? Make sure we don't get ourselves into a
	// state where we report twice.
	if rc.prevErr != nil {
		err = fmt.Errorf("Already saw an error: %v", rc.prevErr)
		return
	}

	// Call through.
	err = rc.Wrapped.Close()
	if err != nil {
		rc.Report(err)
		rc.prevErr = err
		return
	}

	return
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
	var report reqtrace.ReportFunc

	// Start a span.
	desc := fmt.Sprintf("Read: %s", sanitizeObjectName(req.Name))
	ctx, report = reqtrace.StartSpan(ctx, desc)

	// Call the wrapped bucket.
	rc, err = b.Wrapped.NewReader(ctx, req)

	// If the bucket failed, we must report that now.
	if err != nil {
		report(err)
		return
	}

	// Snoop on the outcome.
	rc = &reportingReadCloser{
		Wrapped: rc,
		Report:  report,
	}

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
