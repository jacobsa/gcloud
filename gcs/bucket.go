// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"

	"golang.org/x/net/context"
	storagev1 "google.golang.org/api/storage/v1"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

// A request to create an object, accepted by Bucket.CreateObject.
type CreateObjectRequest struct {
	// Attributes with which the object should be created. The Name field must be
	// set; other zero-valued fields are ignored.
	//
	// Object names must:
	//
	// *  be non-empty.
	// *  be no longer than 1024 bytes.
	// *  be valid UTF-8.
	// *  not contain the code point U+000A (line feed).
	// *  not contain the code point U+000D (carriage return).
	//
	// See here for authoritative documentation:
	//     https://cloud.google.com/storage/docs/bucket-naming#objectnames
	Attrs storage.ObjectAttrs

	// A reader from which to obtain the contents of the object. Must be non-nil.
	Contents io.Reader
}

// Bucket represents a GCS bucket, pre-bound with a bucket name and necessary
// authorization information.
//
// Each method that may block accepts a context object that is used for
// deadlines and cancellation. Users need not package authorization information
// into the context object (using cloud.WithContext or similar).
type Bucket interface {
	Name() string

	// List the objects in the bucket that meet the criteria defined by the
	// query, returning a result object that contains the results and potentially
	// a cursor for retrieving the next portion of the larger set of results.
	ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error)

	// Create a reader for the contents of the object with the given name. The
	// caller must arrange for the reader to be closed when it is no longer
	// needed.
	NewReader(ctx context.Context, objectName string) (io.ReadCloser, error)

	// Create or overwrite an object according to the supplied request. The new
	// object is guaranteed to exist immediately for the purposes of reading (and
	// eventually for listing) after this method returns a nil error. It is
	// guaranteed not to exist before req.Contents returns io.EOF.
	CreateObject(ctx context.Context, req *CreateObjectRequest) (*storage.Object, error)

	// Delete the object with the given name.
	DeleteObject(ctx context.Context, name string) error
}

type bucket struct {
	projID string
	client *http.Client
	name   string
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) ListObjects(ctx context.Context, query *storage.Query) (*storage.Objects, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.ListObjects(authContext, b.name, query)
}

func (b *bucket) NewReader(ctx context.Context, objectName string) (io.ReadCloser, error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.NewReader(authContext, b.name, objectName)
}

func toRawAcls(in []storage.ACLRule) []*storagev1.ObjectAccessControl {
	out := make([]*storagev1.ObjectAccessControl, len(in))
	for i, rule := range in {
		out[i] = &storagev1.ObjectAccessControl{
			Entity: string(rule.Entity),
			Role:   string(rule.Role),
		}
	}

	return out
}

func fromRawAcls(in []*storagev1.ObjectAccessControl) []storage.ACLRule

func fromRawObject(
	bucketName string,
	in *storagev1.Object) (out *storage.Object, err error) {
	// Convert the easy fields.
	out = &storage.Object{
		Bucket:          bucketName,
		Name:            in.Name,
		ContentType:     in.ContentType,
		ContentLanguage: in.ContentLanguage,
		CacheControl:    in.CacheControl,
		ACL:             fromRawAcls(in.Acl),
		ContentEncoding: in.ContentEncoding,
		Size:            int64(in.Size),
		MediaLink:       in.MediaLink,
		Metadata:        in.Metadata,
		Generation:      in.Generation,
		MetaGeneration:  in.Metageneration,
		StorageClass:    in.StorageClass,
		Deleted:         in.TimeDeleted,
		Updated:         in.Updated,
	}

	// Handle special cases.
	if in.Owner != nil {
		out.Owner = in.Owner.Entity
	}

	if out.MD5, err = base64.StdEncoding.DecodeString(in.Md5Hash); err != nil {
		err = fmt.Errorf("Decoding Md5Hash field: %v", err)
		return
	}

	crc32cString, err := base64.StdEncoding.DecodeString(in.Crc32c)
	if err != nil {
		err = fmt.Errorf("Decoding Crc32c field: %v", err)
		return
	}

	if len(crc32cString) != 4 {
		err = fmt.Errorf("Wrong length for decoded Crc32c field: %d", len(crc32cString))
		return
	}

	out.CRC32C =
		uint32(crc32cString[0])<<24 |
			uint32(crc32cString[1])<<16 |
			uint32(crc32cString[2])<<8 |
			uint32(crc32cString[3])<<0

	return
}

func getRawService(authContext context.Context) *storagev1.Service

func (b *bucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *storage.Object, err error) {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	objectsService := getRawService(authContext).Objects

	// As of 2015-02, the wrapped storage package doesn't check this for us,
	// causing silently transformed names:
	//     https://github.com/GoogleCloudPlatform/gcloud-golang/issues/111
	if !utf8.ValidString(req.Attrs.Name) {
		err = errors.New("Invalid object name: not valid UTF-8")
		return
	}

	// Set up an object struct based on the supplied attributes.
	inputObj := &storagev1.Object{
		Name:            req.Attrs.Name,
		Bucket:          b.Name(),
		ContentType:     req.Attrs.ContentType,
		ContentLanguage: req.Attrs.ContentLanguage,
		ContentEncoding: req.Attrs.ContentEncoding,
		CacheControl:    req.Attrs.CacheControl,
		Acl:             toRawAcls(req.Attrs.ACL),
		Metadata:        req.Attrs.Metadata,
	}

	// Configure a 'call' object.
	call := objectsService.Insert(b.Name(), inputObj)
	call.Media(req.Contents)
	call.Projection("full")

	// Execute the call.
	rawObject, err := call.Do()
	if err != nil {
		return
	}

	// Convert the returned object.
	o, err = fromRawObject(b.Name(), rawObject)
	if err != nil {
		return
	}

	return
}

func (b *bucket) DeleteObject(ctx context.Context, name string) error {
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	return storage.DeleteObject(authContext, b.name, name)
}
