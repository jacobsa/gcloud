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
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"
)

var fDebug = flag.Bool(
	"gcs.debug",
	false,
	"Write FUSE debugging messages to stderr.")

var gLogger *log.Logger
var gLoggerOnce sync.Once

func initLogger() {
	const flags = log.Ldate | log.Ltime | log.Lmicroseconds
	gLogger = log.New(os.Stderr, "gcs: ", flags)
}

// If debugging is enabled, wrap the supplied bucket in a layer that prints
// debug messages.
func newDebugBucket(wrapped Bucket) (b Bucket) {
	b = wrapped

	if *fDebug {
		gLoggerOnce.Do(initLogger)
		b = &debugBucket{
			logger:  gLogger,
			wrapped: b,
		}

		return
	}

	return
}

type debugBucket struct {
	logger  *log.Logger
	wrapped Bucket
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (b *debugBucket) mintRequestID() (id uint64) {
	panic("TODO")
}

func (b *debugBucket) requestLogf(
	id uint64,
	format string,
	v ...interface{}) {
	panic("TODO")
}

func (b *debugBucket) startRequest(
	format string,
	v ...interface{}) (id uint64, desc string, start time.Time) {
	start = time.Now()
	id = b.mintRequestID()
	desc = fmt.Sprintf(format, v...)

	b.requestLogf(id, "<- %s", desc)
	return
}

func (b *debugBucket) finishRequest(
	id uint64,
	desc string,
	start time.Time,
	err *error) {
	duration := time.Since(start)

	errDesc := "OK"
	if *err != nil {
		errDesc = (*err).Error()
	}

	b.requestLogf(id, "-> %s (%v): %s", desc, duration, errDesc)
}

////////////////////////////////////////////////////////////////////////
// Bucket interface
////////////////////////////////////////////////////////////////////////

func (b *debugBucket) Name() string {
	return b.wrapped.Name()
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
	id, desc, start := b.startRequest("CreateObject(%q)", req.Name)
	defer b.finishRequest(id, desc, start, &err)

	o, err = b.wrapped.CreateObject(ctx, req)
	return
}

func (b *debugBucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	id, desc, start := b.startRequest("StatObject(%q)", req.Name)
	defer b.finishRequest(id, desc, start, &err)

	o, err = b.wrapped.StatObject(ctx, req)
	return
}

func (b *debugBucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	id, desc, start := b.startRequest("ListObjects()")
	defer b.finishRequest(id, desc, start, &err)

	listing, err = b.wrapped.ListObjects(ctx, req)
	return
}

func (b *debugBucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	id, desc, start := b.startRequest("UpdateObject(%q)", req.Name)
	defer b.finishRequest(id, desc, start, &err)

	o, err = b.wrapped.UpdateObject(ctx, req)
	return
}

func (b *debugBucket) DeleteObject(
	ctx context.Context,
	name string) (err error) {
	id, desc, start := b.startRequest("DeleteObject(%q)", name)
	defer b.finishRequest(id, desc, start, &err)

	err = b.wrapped.DeleteObject(ctx, name)
	return
}
