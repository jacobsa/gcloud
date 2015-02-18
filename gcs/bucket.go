// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package gcs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
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
	projID     string
	client     *http.Client
	rawService *storagev1.Service
	name       string
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

func fromRawAcls(in []*storagev1.ObjectAccessControl) []storage.ACLRule {
	out := make([]storage.ACLRule, len(in))
	for i, rule := range in {
		out[i] = storage.ACLRule{
			Entity: storage.ACLEntity(rule.Entity),
			Role:   storage.ACLRole(rule.Role),
		}
	}

	return out
}

func fromRfc3339(s string) (t time.Time, err error) {
	if s != "" {
		t, err = time.Parse(time.RFC3339, s)
	}

	return
}

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
	}

	// Handle special cases.
	if in.Owner != nil {
		out.Owner = in.Owner.Entity
	}

	if out.Deleted, err = fromRfc3339(in.TimeDeleted); err != nil {
		err = fmt.Errorf("Decoding TimeDeleted field: %v", err)
		return
	}

	if out.Updated, err = fromRfc3339(in.Updated); err != nil {
		err = fmt.Errorf("Decoding Updated field: %v", err)
		return
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

type contentTypeReader struct {
	wrapped     io.Reader
	contentType string
}

var _ io.Reader = &contentTypeReader{}
var _ googleapi.ContentTyper = &contentTypeReader{}

func (r *contentTypeReader) Read(p []byte) (int, error) {
	return r.wrapped.Read(p)
}

func (r *contentTypeReader) ContentType() string {
	return r.contentType
}

func makeMedia(req *CreateObjectRequest) (r io.Reader) {
	r = req.Contents

	// Work around the following interaction:
	//
	// 1. The storage package attempts to sniff the content type from the media,
	// regardless of whether Object.ContentType has been explicitly set (cf.
	// http://goo.gl/Dlvq7j).
	//
	// 2. The sniffed content type goes into the HTTP headers and the explicit
	// ContentType field goes into the JSON body.
	//
	// 3. GCS apparently ignores the JSON body content type and stores the one
	// from the HTTP headers (cf. Google-internal bug ID 19416462).
	if req.Attrs.ContentType != "" {
		r = &contentTypeReader{r, req.Attrs.ContentType}
	}

	return
}

func (b *bucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *storage.Object, err error) {
	// TODO(jacobsa): Refactor so that this is done once by NewConn.
	rawService, err := storagev1.New(b.client)
	if err != nil {
		err = fmt.Errorf("storagev1.New: %v", err)
		return
	}

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
	call := rawService.Objects.Insert(b.Name(), inputObj)
	call.Media(makeMedia(req))
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
