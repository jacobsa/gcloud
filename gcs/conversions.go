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
	"encoding/base64"
	"fmt"
	"time"

	storagev1 "google.golang.org/api/storage/v1"
	"google.golang.org/cloud/storage"
)

////////////////////////////////////////////////////////////////////////
// To our types
////////////////////////////////////////////////////////////////////////

func toTime(s string) (t time.Time, err error) {
	if s != "" {
		t, err = time.Parse(time.RFC3339, s)
	}

	return
}

func toObjects(in []*storagev1.Object) (out []*Object, err error) {
	for _, rawObject := range in {
		var o *Object
		o, err = toObject(rawObject)
		if err != nil {
			err = fmt.Errorf("toObject: %v", err)
			return
		}

		out = append(out, o)
	}

	return
}

func toListing(in *storagev1.Objects) (out *Listing, err error) {
	out = &Listing{
		CollapsedRuns:     in.Prefixes,
		ContinuationToken: in.NextPageToken,
	}

	out.Objects, err = toObjects(in.Items)
	if err != nil {
		err = fmt.Errorf("toObjects: %v", err)
		return
	}

	return
}

func toObject(in *storagev1.Object) (out *Object, err error) {
	// Convert the easy fields.
	out = &Object{
		Name:            in.Name,
		ContentType:     in.ContentType,
		ContentLanguage: in.ContentLanguage,
		CacheControl:    in.CacheControl,
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

	if out.Deleted, err = toTime(in.TimeDeleted); err != nil {
		err = fmt.Errorf("Decoding TimeDeleted field: %v", err)
		return
	}

	if out.Updated, err = toTime(in.Updated); err != nil {
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
		err = fmt.Errorf(
			"Wrong length for decoded Crc32c field: %d",
			len(crc32cString))

		return
	}

	out.CRC32C =
		uint32(crc32cString[0])<<24 |
			uint32(crc32cString[1])<<16 |
			uint32(crc32cString[2])<<8 |
			uint32(crc32cString[3])<<0

	return
}

// TODO(jacobsa): Delete this when possible. See issue #4.
func fromWrappedObject(in *storage.Object) (out *Object) {
	out = &Object{
		Name:            in.Name,
		ContentType:     in.ContentType,
		ContentLanguage: in.ContentLanguage,
		CacheControl:    in.CacheControl,
		Owner:           in.Owner,
		ContentEncoding: in.ContentEncoding,
		MD5:             in.MD5,
		CRC32C:          in.CRC32C,
		Size:            in.Size,
		MediaLink:       in.MediaLink,
		Metadata:        in.Metadata,
		Generation:      in.Generation,
		MetaGeneration:  in.MetaGeneration,
		StorageClass:    in.StorageClass,
		Deleted:         in.Deleted,
		Updated:         in.Updated,
	}

	return
}

////////////////////////////////////////////////////////////////////////
// From our types
////////////////////////////////////////////////////////////////////////

func toRawObject(
	bucketName string,
	in *CreateObjectRequest) (out *storagev1.Object, err error) {
	out = &storagev1.Object{
		Bucket:          bucketName,
		Name:            in.Name,
		ContentType:     in.ContentType,
		ContentLanguage: in.ContentLanguage,
		ContentEncoding: in.ContentEncoding,
		CacheControl:    in.CacheControl,
		Metadata:        in.Metadata,
	}

	return
}
