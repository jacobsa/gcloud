// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package syncutil

import (
	"sync"

	"golang.org/x/net/context"
)

// XXX: Comments
type Bundle struct {
	context context.Context
	cancel  context.CancelFunc

	waitGroup sync.WaitGroup

	errorOnce  sync.Once
	firstError error
}

// XXX: Comments
func (b *Bundle) Add(f func(context.Context) error) {
	b.waitGroup.Add(1)

	// Run the function in the background.
	go func() {
		defer b.waitGroup.Done()

		err := f(b.context)
		if err == nil {
			return
		}

		// On first error, cancel the context and save the error.
		b.errorOnce.Do(func() {
			b.firstError = err
			b.cancel()
		})
	}()
}

// XXX: Comments
func (b *Bundle) Join() error {
	b.waitGroup.Wait()
	return b.firstError
}

// XXX: Comments for interface and impl
func NewBundle(parent context.Context) *Bundle {
	if parent == nil {
		parent = context.Background()
	}

	b := &Bundle{}
	b.context, b.cancel = context.WithCancel(parent)

	return b
}
