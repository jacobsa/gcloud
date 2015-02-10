// Copyright 2015 Google Inc. All Rights Reserved.
// Author: jacobsa@google.com (Aaron Jacobs)

package syncutil_test

import (
	"errors"
	"testing"

	"github.com/jacobsa/gcloud/syncutil"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"golang.org/x/net/context"
)

func TestOgletest(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type BundleTest struct {
	bundle       *syncutil.Bundle
	cancelParent context.CancelFunc
}

func init() { RegisterTestSuite(&BundleTest{}) }

func (t *BundleTest) SetUp(ti *TestInfo) {
	// Set up the parent context.
	parentCtx, cancelParent := context.WithCancel(context.Background())
	t.cancelParent = cancelParent

	// Set up the bundle.
	t.bundle = syncutil.NewBundle(parentCtx)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *BundleTest) NoOperations() {
	ExpectEq(nil, t.bundle.Join())
}

func (t *BundleTest) SingleOp_Success() {
	t.bundle.Add(func(c context.Context) error {
		return nil
	})

	ExpectEq(nil, t.bundle.Join())
}

func (t *BundleTest) SingleOp_Error() {
	expected := errors.New("taco")
	t.bundle.Add(func(c context.Context) error {
		return expected
	})

	ExpectEq(expected, t.bundle.Join())
}

func (t *BundleTest) SingleOp_ParentCancelled() {
	// Start an op that waits for the context to be cancelled before returning an
	// expected value.
	expected := errors.New("taco")
	t.bundle.Add(func(c context.Context) error {
		<-c.Done()
		return expected
	})

	// Cancel the parent context, then join the bundle. The op should see the
	// cancellation, so we shouldn't deadlock and we should get the expected
	// value.
	t.cancelParent()
	ExpectEq(expected, t.bundle.Join())
}

func (t *BundleTest) MultipleOps_Success() {
	for i := 0; i < 4; i++ {
		t.bundle.Add(func(c context.Context) error {
			return nil
		})
	}

	ExpectEq(nil, t.bundle.Join())
}

func (t *BundleTest) MultipleOps_UnorderedErrors() {
	// Start multiple ops, each returning a different error.
	errs := []error{
		errors.New("taco"),
		errors.New("burrito"),
		errors.New("enchilada"),
	}

	for i := 0; i < len(errs); i++ {
		iCopy := i
		t.bundle.Add(func(c context.Context) error {
			return errs[iCopy]
		})
	}

	// Joining the bundle should result in some error from the list.
	ExpectThat(errs, Contains(t.bundle.Join()))
}

func (t *BundleTest) MultipleOps_OneError_OthersDontWait() {
	expected := errors.New("taco")

	// Add two operations that succeed and one that fails.
	t.bundle.Add(func(c context.Context) error { return nil })
	t.bundle.Add(func(c context.Context) error { return expected })
	t.bundle.Add(func(c context.Context) error { return nil })

	// We should see the failure.
	ExpectEq(expected, t.bundle.Join())
}

func (t *BundleTest) MultipleOps_OneError_OthersWaitForCancellation() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_ParentCancelled() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_PreviousError_NewOpsObserve() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) MultipleOps_PreviousParentCancel_NewOpsObserve() {
	AssertFalse(true, "TODO")
}

func (t *BundleTest) JoinWaitsForAllOps() {
	AssertFalse(true, "TODO")
}
