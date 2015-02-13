// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package oauthutil

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"
)

const authCodeFlagName = "oauthutil.auth_code"

var fAuthCode = flag.String(
	authCodeFlagName,
	"",
	"Auth code from Google developer console.")

var fDebugHttp = flag.Bool(
	"oauthutil.debug_http",
	false,
	"Dump information about HTTP requests.")

// Set up an authenticated HTTP client that retrieves tokens according to the
// supplied config, caching them in the user's home directory with the given
// file name.
//
// If a token cannot be obtained because there is no cache entry or no refresh
// token within the cache entry, the program will be halted and a message
// printed for the user with instructions on how to obtain an authorization
// code and feed it to the program via a flag.
func NewTerribleHttpClient(
	config *oauth2.Config,
	cacheFileName string) (*http.Client, error) {
	// Create a token source.
	tokenSource, err := NewTerribleTokenSource(
		config,
		flag.Lookup(authCodeFlagName),
		cacheFileName)

	if err != nil {
		return nil, err
	}

	// Ensure that we fail early if misconfigured, by requesting an initial
	// token.
	if _, err := tokenSource.Token(); err != nil {
		return nil, fmt.Errorf("Getting initial OAuth token: %v", err)
	}

	// Create the HTTP transport.
	transport := &oauth2.Transport{
		Source: tokenSource,
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
