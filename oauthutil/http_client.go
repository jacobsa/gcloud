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

package oauthutil

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var fDebugHttp = flag.Bool(
	"oauthutil.debug_http",
	false,
	"Dump information about HTTP requests.")

// Set up an authenticated HTTP client that fetches tokens using the OAuth 2.0
// JSON Web Token flow ("two-legged OAuth 2.0"). The path must point at a
// readable JSON key file for a service account downloaded from the Google
// Developers Console.
func NewJWTHttpClient(jsonPath string, scopes []string) (*http.Client, error) {
	// Attempt to read the JSON file.
	contents, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		return nil, err
	}

	// Create a config struct based on its contents.
	jwtConfig, err := google.JWTConfigFromJSON(contents, scopes...)
	if err != nil {
		return nil, err
	}

	// Create the HTTP transport.
	transport := &oauth2.Transport{
		Source: jwtConfig.TokenSource(oauth2.NoContext),
		Base:   http.DefaultTransport,
	}

	// Enable debugging if requested.
	if *fDebugHttp {
		transport.Base = &debuggingTransport{wrapped: transport.Base}
	}

	return &http.Client{Transport: transport}, nil
}

////////////////////////////////////////////////////////////////////////
// HTTP debugging
////////////////////////////////////////////////////////////////////////

func readAllAndClose(rc io.ReadCloser) string {
	// Read.
	contents, err := ioutil.ReadAll(rc)
	if err != nil {
		panic(err)
	}

	// Close.
	if err := rc.Close(); err != nil {
		panic(err)
	}

	return string(contents)
}

// Read everything from *rc, then replace it with a copy.
func snarfBody(rc *io.ReadCloser) string {
	contents := readAllAndClose(*rc)
	*rc = ioutil.NopCloser(bytes.NewBufferString(contents))
	return contents
}

type debuggingTransport struct {
	wrapped http.RoundTripper
}

func (t *debuggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Print information about the request.
	fmt.Println("========== REQUEST ===========")
	fmt.Println(req.Method, req.URL, req.Proto)
	for k, vs := range req.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if req.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&req.Body))
	}

	// Execute the request.
	res, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return res, err
	}

	// Print the response.
	fmt.Println("========== RESPONSE ==========")
	fmt.Println(res.Proto, res.Status)
	for k, vs := range res.Header {
		for _, v := range vs {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	if res.Body != nil {
		fmt.Printf("\n%s\n", snarfBody(&res.Body))
	}
	fmt.Println("==============================")

	return res, err
}
