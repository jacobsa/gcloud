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

// A tool to measure the upload throughput of GCS.
package main

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jacobsa/fuse/fsutil"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/oauthutil"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

var fBucket = flag.String("bucket", "", "Name of bucket.")
var fKeyFile = flag.String("key_file", "", "Path to JSON key file.")
var fSize = flag.Int64("size", 1<<26, "Size of content to write.")
var fFile = flag.String("file", "", "If set, use pre-existing contents.")

func createBucket() (bucket gcs.Bucket, err error) {
	// Create an authenticated HTTP client.
	if *fKeyFile == "" {
		err = errors.New("You must set --key_file.")
		return
	}

	httpClient, err := oauthutil.NewJWTHttpClient(
		*fKeyFile,
		[]string{storage.ScopeFullControl})

	if err != nil {
		err = fmt.Errorf("NewJWTHttpClient: %v", err)
		return
	}

	// Use that to create a connection.
	conn, err := gcs.NewConn("", httpClient)
	if err != nil {
		err = fmt.Errorf("NewConn: %v", err)
		return
	}

	// Extract the bucket.
	if *fBucket == "" {
		err = errors.New("You must set --bucket.")
		return
	}

	bucket = conn.GetBucket(*fBucket)

	return
}

func getFile() (f *os.File, err error) {
	// Is there a pre-set file?
	if *fFile != "" {
		f, err = os.OpenFile(*fFile, os.O_RDONLY, 0)
		if err != nil {
			err = fmt.Errorf("OpenFile: %v", err)
			return
		}

		return
	}

	// Create a temporary file to hold random contents.
	f, err = fsutil.AnonymousFile("")
	if err != nil {
		err = fmt.Errorf("AnonymousFile: %v", err)
		return
	}

	// Copy a bunch of random data into the file.
	log.Println("Reading random data.")
	_, err = io.Copy(f, io.LimitReader(rand.Reader, *fSize))
	if err != nil {
		err = fmt.Errorf("Copy: %v", err)
		return
	}

	// Seek back to the start for consumption.
	_, err = f.Seek(0, 0)
	if err != nil {
		err = fmt.Errorf("Seek: %v", err)
		return
	}

	return
}

func run() (err error) {
	bucket, err := createBucket()
	if err != nil {
		err = fmt.Errorf("createBucket: %v", err)
		return
	}

	// Get an appropriate file.
	f, err := getFile()
	if err != nil {
		err = fmt.Errorf("getFile: %v", err)
		return
	}

	// Create an object using the contents of the file.
	log.Println("Creating object.")
	req := &gcs.CreateObjectRequest{
		Attrs: storage.ObjectAttrs{
			Name: "foo",
		},
		Contents: f,
	}

	before := time.Now()
	_, err = bucket.CreateObject(context.Background(), req)
	if err != nil {
		err = fmt.Errorf("CreateObject: %v", err)
		return
	}

	log.Printf("Wrote object in %v.", *fSize, time.Since(before))

	return
}

func main() {
	flag.Parse()

	err := run()
	if err != nil {
		log.Fatalln(err)
	}
}
