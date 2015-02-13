// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package oauthutil

import (
	"net/http"

	"golang.org/x/oauth2"
)

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
	cacheFileName string) (*http.Client, error)
