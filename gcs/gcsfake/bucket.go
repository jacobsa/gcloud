// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcsfake

import (
	"io"

	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

// Create an in-memory bucket with the given name and empty contents.
func NewFakeBucket(name string) gcs.Bucket {
	return &bucket{name: name}
}

type bucket struct {
	name string
}

func (b *bucket) Name() string

func (b *bucket) ListObjects(
	ctx context.Context,
	query *storage.Query) (*storage.Objects, error)

func (b *bucket) NewReader(
	ctx context.Context,
	objectName string) (io.ReadCloser, error)

func (b *bucket) NewWriter(
	ctx context.Context,
	attrs *storage.ObjectAttrs) (gcs.ObjectWriter, error)

func (b *bucket) DeleteObject(
	ctx context.Context,
	name string) error
