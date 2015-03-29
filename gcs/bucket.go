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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/jacobsa/gcloud/httputil"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	storagev1 "google.golang.org/api/storage/v1"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

const userAgent = "github.com-jacobsa-gloud-gcs"

// Bucket represents a GCS bucket, pre-bound with a bucket name and necessary
// authorization information.
//
// Each method that may block accepts a context object that is used for
// deadlines and cancellation. Users need not package authorization information
// into the context object (using cloud.WithContext or similar).
type Bucket interface {
	Name() string

	// Create a reader for the contents of a particular generation of an object.
	// The caller must arrange for the reader to be closed when it is no longer
	// needed.
	//
	// If the object doesn't exist, err will be of type *NotFoundError.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/get
	NewReader(
		ctx context.Context,
		req *ReadObjectRequest) (io.ReadCloser, error)

	// Create or overwrite an object according to the supplied request. The new
	// object is guaranteed to exist immediately for the purposes of reading (and
	// eventually for listing) after this method returns a nil error. It is
	// guaranteed not to exist before req.Contents returns io.EOF.
	//
	// If the request fails due to a precondition not being met, the error will
	// be of type *PreconditionError.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/insert
	//     https://cloud.google.com/storage/docs/json_api/v1/how-tos/upload
	CreateObject(
		ctx context.Context,
		req *CreateObjectRequest) (*Object, error)

	// Return current information about the object with the given name.
	//
	// If the object doesn't exist, err will be of type *NotFoundError.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/get
	StatObject(
		ctx context.Context,
		req *StatObjectRequest) (*Object, error)

	// List the objects in the bucket that meet the criteria defined by the
	// request, returning a result object that contains the results and
	// potentially a cursor for retrieving the next portion of the larger set of
	// results.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/list
	ListObjects(
		ctx context.Context,
		req *ListObjectsRequest) (*Listing, error)

	// Update the object specified by newAttrs.Name, patching using the non-zero
	// fields of newAttrs.
	//
	// If the object doesn't exist, err will be of type *NotFoundError.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/patch
	UpdateObject(
		ctx context.Context,
		req *UpdateObjectRequest) (*Object, error)

	// Delete the object with the given name.
	//
	// If the object doesn't exist, err will be of type *NotFoundError.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/delete
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

func (b *bucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	// Set up the query for the URL.
	query := make(url.Values)
	query.Set("projection", "full")

	if req.Prefix != "" {
		query.Set("prefix", req.Prefix)
	}

	if req.Delimiter != "" {
		query.Set("delimiter", req.Delimiter)
	}

	if req.ContinuationToken != "" {
		query.Set("pageToken", req.ContinuationToken)
	}

	if req.MaxResults != 0 {
		query.Set("maxResults", fmt.Sprintf("%v", req.MaxResults))
	}

	// Construct an appropriate URL (cf. http://goo.gl/aVSAhT).
	opaque := fmt.Sprintf(
		"//www.googleapis.com/storage/v1/b/%s/o",
		httputil.EncodePathSegment(b.Name()))

	url := &url.URL{
		Scheme:   "https",
		Opaque:   opaque,
		RawQuery: query.Encode(),
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("GET", url, nil, userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Call the server.
	httpRes, err := b.client.Do(httpReq)
	if err != nil {
		err = fmt.Errorf("HTTP client: %v", err)
		return
	}

	defer googleapi.CloseBody(httpRes)

	// Check for HTTP-level errors.
	if err = googleapi.CheckResponse(httpRes); err != nil {
		// Special case: handle not found errors.
		if typed, ok := err.(*googleapi.Error); ok {
			if typed.Code == http.StatusNotFound {
				err = &NotFoundError{Err: typed}
			}
		}

		return
	}

	// Parse the response.
	var rawListing *storagev1.Objects
	if err = json.NewDecoder(httpRes.Body).Decode(&rawListing); err != nil {
		return
	}

	// Convert the response.
	if listing, err = toListing(rawListing); err != nil {
		return
	}

	return
}

func (b *bucket) NewReader(
	ctx context.Context,
	req *ReadObjectRequest) (rc io.ReadCloser, err error) {
	// Construct an appropriate URL.
	//
	// The documentation (http://goo.gl/gZo4oj) is extremely vague about how this
	// is supposed to work. As of 2015-03-13, it says only that "for most
	// operations" you can use the following form to access an object:
	//
	//     storage.googleapis.com/<bucket>/<object>
	//
	// In Google-internal bug 19718068, it was clarified that the intent is that
	// each of the bucket and object names are encoded into a single path
	// segment, as defined by RFC 3986.
	bucketSegment := httputil.EncodePathSegment(b.name)
	objectSegment := httputil.EncodePathSegment(req.Name)
	opaque := fmt.Sprintf(
		"//storage.googleapis.com/%s/%s",
		bucketSegment,
		objectSegment)

	url := &url.URL{
		Scheme: "https",
		Opaque: opaque,
	}

	// Add a generation condition, if specified.
	if req.Generation != 0 {
		url.RawQuery = fmt.Sprintf("generation=%v", req.Generation)
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("GET", url, nil, userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Call the server.
	httpRes, err := b.client.Do(httpReq)
	if err != nil {
		err = fmt.Errorf("HTTP client: %v", err)
		return
	}

	// Check for HTTP error statuses.
	if err = googleapi.CheckResponse(httpRes); err != nil {
		googleapi.CloseBody(httpRes)

		// Special case: handle not found errors.
		if typed, ok := err.(*googleapi.Error); ok {
			if typed.Code == http.StatusNotFound {
				err = &NotFoundError{Err: typed}
			}
		}

		return
	}

	// The body contains the object data.
	rc = httpRes.Body

	return
}

func (b *bucket) CreateObject(
	ctx context.Context,
	req *CreateObjectRequest) (o *Object, err error) {
	o, err = createObject(b.client, b.Name(), ctx, req)
	return
}

func (b *bucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	// Call the wrapped package.
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	wrappedObj, err := storage.StatObject(authContext, b.name, req.Name)

	// Transform errors.
	if err == storage.ErrObjectNotExist {
		err = &NotFoundError{
			Err: err,
		}

		return
	}

	// Transform the object.
	o = fromWrappedObject(wrappedObj)

	return
}

func (b *bucket) UpdateObject(
	ctx context.Context,
	req *UpdateObjectRequest) (o *Object, err error) {
	// Set up a map representing the JSON object we want to send to GCS. For now,
	// we don't treat empty strings specially.
	jsonMap := make(map[string]interface{})

	if req.ContentType != nil {
		jsonMap["contentType"] = req.ContentType
	}

	if req.ContentEncoding != nil {
		jsonMap["contentEncoding"] = req.ContentEncoding
	}

	if req.ContentLanguage != nil {
		jsonMap["contentLanguage"] = req.ContentLanguage
	}

	if req.CacheControl != nil {
		jsonMap["cacheControl"] = req.CacheControl
	}

	// Implement the convention that a pointer to an empty string means to delete
	// the field (communicated to GCS by setting it to null in the JSON).
	for k, v := range jsonMap {
		if *(v.(*string)) == "" {
			jsonMap[k] = nil
		}
	}

	// Add a field for user metadata if appropriate.
	if req.Metadata != nil {
		jsonMap["metadata"] = req.Metadata
	}

	// Set up a reader for the JSON object.
	body, err := googleapi.WithoutDataWrapper.JSONReader(jsonMap)
	if err != nil {
		return
	}

	// Set up URL params.
	urlParams := make(url.Values)
	urlParams.Set("projection", "full")

	// Set up the URL with a tempalte that we will later expand.
	url := googleapi.ResolveRelative(
		b.rawService.BasePath,
		"b/{bucket}/o/{object}")

	url += "?" + urlParams.Encode()

	// Create an HTTP request using NewRequest, which parses the URL string.
	// Expand the URL object it creates.
	httpReq, err := httputil.NewRequest("PATCH", url, body, userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	googleapi.Expand(
		httpReq.URL,
		map[string]string{
			"bucket": b.Name(),
			"object": req.Name,
		})

	// Set up HTTP request headers.
	httpReq.Header.Set("Content-Type", "application/json")

	// Execute the HTTP request.
	httpRes, err := b.client.Do(httpReq)
	if err != nil {
		return
	}

	defer googleapi.CloseBody(httpRes)

	// Check for HTTP-level errors.
	if err = googleapi.CheckResponse(httpRes); err != nil {
		// Special case: handle not found errors.
		if typed, ok := err.(*googleapi.Error); ok {
			if typed.Code == http.StatusNotFound {
				err = &NotFoundError{Err: typed}
			}
		}

		return
	}

	// Parse the response.
	var rawObject *storagev1.Object
	if err = json.NewDecoder(httpRes.Body).Decode(&rawObject); err != nil {
		return
	}

	// Convert the response.
	if o, err = toObject(rawObject); err != nil {
		err = fmt.Errorf("toObject: %v", err)
		return
	}

	return
}

func (b *bucket) DeleteObject(ctx context.Context, name string) (err error) {
	// Call the wrapped package.
	authContext := cloud.WithContext(ctx, b.projID, b.client)
	err = storage.DeleteObject(authContext, b.name, name)

	// Transform errors.
	if err == storage.ErrObjectNotExist {
		err = &NotFoundError{
			Err: err,
		}
	}

	// Handle the fact that as of 2015-03-12, the wrapped package does not
	// correctly return ErrObjectNotExist here.
	if typed, ok := err.(*googleapi.Error); ok {
		if typed.Code == http.StatusNotFound {
			err = &NotFoundError{Err: typed}
		}
	}

	return
}

func newBucket(
	projID string,
	client *http.Client,
	rawService *storagev1.Service,
	name string) Bucket {
	return &bucket{
		projID:     projID,
		client:     client,
		rawService: rawService,
		name:       name,
	}
}
