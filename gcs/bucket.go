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
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/jacobsa/gcloud/httputil"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	storagev1 "google.golang.org/api/storage/v1"
)

// Bucket represents a GCS bucket, pre-bound with a bucket name and necessary
// authorization information.
//
// Each method that may block accepts a context object that is used for
// deadlines and cancellation. Users need not package authorization information
// into the context object (using cloud.WithContext or similar).
//
// All methods are safe for concurrent access.
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

	// Delete the object with the given name. Non-existence of the object is not
	// treated as an error.
	//
	// Official documentation:
	//     https://cloud.google.com/storage/docs/json_api/v1/objects/delete
	DeleteObject(ctx context.Context, name string) error
}

type bucket struct {
	client    *http.Client
	userAgent string
	name      string
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) ListObjects(
	ctx context.Context,
	req *ListObjectsRequest) (listing *Listing, err error) {
	// Construct an appropriate URL (cf. http://goo.gl/aVSAhT).
	opaque := fmt.Sprintf(
		"//www.googleapis.com/storage/v1/b/%s/o",
		httputil.EncodePathSegment(b.Name()))

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

	url := &url.URL{
		Scheme:   "https",
		Host:     "www.googleapis.com",
		Opaque:   opaque,
		RawQuery: query.Encode(),
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("GET", url, nil, b.userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Call the server.
	httpRes, err := httputil.Do(ctx, b.client, httpReq)
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
	// The documentation (http://goo.gl/JKf25r) is extremely vague about how this
	// is supposed to work. As of 2015-03-25, it has no prose but gives the
	// example:
	//
	//     GET https://www.googleapis.com/storage/v1/b/<bucket>/o/<object>
	//
	// In Google-internal bug 19718068, it was clarified that the intent is that
	// each of the bucket and object names are encoded into a single path
	// segment, as defined by RFC 3986.
	bucketSegment := httputil.EncodePathSegment(b.name)
	objectSegment := httputil.EncodePathSegment(req.Name)
	opaque := fmt.Sprintf(
		"//www.googleapis.com/storage/v1/b/%s/o/%s",
		bucketSegment,
		objectSegment)

	query := make(url.Values)
	query.Set("alt", "media")

	if req.Generation != 0 {
		query.Set("generation", fmt.Sprintf("%v", req.Generation))
	}

	url := &url.URL{
		Scheme:   "https",
		Host:     "www.googleapis.com",
		Opaque:   opaque,
		RawQuery: query.Encode(),
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("GET", url, nil, b.userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Set a Range header, if appropriate.
	var bodyLimit int64
	if req.Range != nil {
		var v string
		v, bodyLimit = makeRangeHeaderValue(*req.Range)
		httpReq.Header.Set("Range", v)
	}

	// Call the server.
	httpRes, err := httputil.Do(ctx, b.client, httpReq)
	if err != nil {
		return
	}

	// Close the body if we're returning in error.
	defer func() {
		if err != nil {
			googleapi.CloseBody(httpRes)
		}
	}()

	// Check for HTTP error statuses.
	if err = googleapi.CheckResponse(httpRes); err != nil {
		if typed, ok := err.(*googleapi.Error); ok {
			// Special case: handle not found errors.
			if typed.Code == http.StatusNotFound {
				err = &NotFoundError{Err: typed}
			}

			// Special case: if the user requested a range and we received HTTP 416
			// from the server, treat this as an empty body. See makeRangeHeaderValue
			// for more details.
			if req.Range != nil &&
				typed.Code == http.StatusRequestedRangeNotSatisfiable {
				err = nil
				googleapi.CloseBody(httpRes)
				rc = ioutil.NopCloser(strings.NewReader(""))
			}
		}

		return
	}

	// The body contains the object data.
	rc = httpRes.Body

	// If the user requested a range and we didn't see HTTP 416 above, we require
	// an HTTP 206 response and must truncate the body. See the notes on
	// makeRangeHeaderValue.
	if req.Range != nil {
		if httpRes.StatusCode != http.StatusPartialContent {
			err = fmt.Errorf(
				"Received unexpected status code %d instead of HTTP 206",
				httpRes.StatusCode)

			return
		}

		rc = &limitReadCloser{
			n:       bodyLimit,
			wrapped: rc,
		}
	}

	return
}

func (b *bucket) StatObject(
	ctx context.Context,
	req *StatObjectRequest) (o *Object, err error) {
	// Construct an appropriate URL (cf. http://goo.gl/MoITmB).
	opaque := fmt.Sprintf(
		"//www.googleapis.com/storage/v1/b/%s/o/%s",
		httputil.EncodePathSegment(b.Name()),
		httputil.EncodePathSegment(req.Name))

	query := make(url.Values)
	query.Set("projection", "full")

	url := &url.URL{
		Scheme:   "https",
		Host:     "www.googleapis.com",
		Opaque:   opaque,
		RawQuery: query.Encode(),
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("GET", url, nil, b.userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Execute the HTTP request.
	httpRes, err := httputil.Do(ctx, b.client, httpReq)
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
	// Construct an appropriate URL (cf. http://goo.gl/TRQJjZ).
	opaque := fmt.Sprintf(
		"//www.googleapis.com/storage/v1/b/%s/o/%s",
		httputil.EncodePathSegment(b.Name()),
		httputil.EncodePathSegment(name))

	url := &url.URL{
		Scheme: "https",
		Host:   "www.googleapis.com",
		Opaque: opaque,
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest("DELETE", url, nil, b.userAgent)
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Execute the HTTP request.
	httpRes, err := httputil.Do(ctx, b.client, httpReq)
	if err != nil {
		return
	}

	defer googleapi.CloseBody(httpRes)

	// Check for HTTP-level errors.
	err = googleapi.CheckResponse(httpRes)

	// Special case: we want deletes to be idempotent.
	if typed, ok := err.(*googleapi.Error); ok {
		if typed.Code == http.StatusNotFound {
			err = nil
		}
	}

	// Propagate other errors.
	if err != nil {
		return
	}

	return
}

func newBucket(
	client *http.Client,
	userAgent string,
	name string) Bucket {
	return &bucket{
		client:    client,
		userAgent: userAgent,
		name:      name,
	}
}

func describeRange(
	start int64,
	limit *int64) (s string) {
	if limit == nil {
		s = fmt.Sprintf("[%d, ∞)", start)
	} else {
		s = fmt.Sprintf("[%d, %d)", start, limit)
	}

	return
}

// Given a [start, limit) range, create an HTTP 1.1 Range header which ensures
// that the resulting body is what the user intended, given the following
// protocol:
//
//  *  If GCS returns HTTP 416 (Requested range not satisfiable), treat the
//     body as if it is empty.
//
//  *  If GCS returns HTTP 206 (Partial Content), truncate the body to at most
//     the returned number of bytes. Do not treat the body already being
//     shorter than this length as an error.
//
//  *  If GCS returns any other status code, regard it as an error.
//
// This monkeying around is necessary because of various shitty aspects of the
// HTTP 1.1 Range header:
//
//  *  Ranges are [min, max] and require min <= max.
//
//  *  Therefore users cannot request empty ranges, even though the status code
//     and headers may still be meaningful without actual entity body content.
//
//  *  min must be less than the entity body length, unless the server is
//     polite enough to send HTTP 416 when this is not the case. Luckily GCS
//     appears to be.
//
// Cf. http://tools.ietf.org/html/rfc2616#section-14.35.1
func makeRangeHeaderValue(br ByteRange) (hdr string, n int64) {
	// Canonicalize ranges that the server will not like. We must do this because
	// RFC 2616 §14.35.1 requires the last byte position to be greater than or
	// equal to the first byte position.
	if br.Limit < br.Start {
		br.Start = 0
		br.Limit = 0
	}

	// HTTP byte range specifiers are [min, max] double-inclusive, ugh. But we
	// require the user to truncate, so there is no harm in requesting one byte
	// extra at the end. If the range GCS sees goes past the end of the object,
	// it truncates. If the range starts after the end of the object, it returns
	// HTTP 416, which we require the user to handle.
	hdr = fmt.Sprintf("bytes=%d-%d", br.Start, br.Limit)
	n = int64(br.Limit - br.Start)

	return
}
