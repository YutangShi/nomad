package nomad

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/require"
)

func TestManager_ManageUpdates(t *testing.T) {
	t.Parallel()

	s, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()

	s1 := mock.EventSink()
	s2 := mock.EventSink()

	require.NoError(t, s.State().UpsertEventSink(1000, s1))
	require.NoError(t, s.State().UpsertEventSink(1001, s2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := NewManager(ctx, []string{s1.ID, s2.ID}, s.State())
	require.NoError(t, err)

	s1.Address = "http://example.com"
	require.NoError(t, s.State().UpsertEventSink(1012, s1))

	select {
	case update := <-manager.UpdateCh():
		require.Equal(t, s1.ID, update.ID)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for manager update trigger")
	}
}

func TestManagedSink_Run_Webhook(t *testing.T) {
	t.Parallel()

	// Setup webhook destination
	received := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var event structs.Events
		dec := json.NewDecoder(r.Body)
		require.NoError(t, dec.Decode(&event))
		require.Equal(t, "Deployment", string(event.Events[0].Topic))

		close(received)
	}))
	defer ts.Close()

	s, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()

	s1 := mock.EventSink()
	s1.Address = ts.URL
	require.NoError(t, s.State().UpsertEventSink(1000, s1))

	ws := memdb.NewWatchSet()
	sink, err := s.State().EventSinkByID(ws, s1.ID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create sink
	mSink, err := NewManagedSink(ctx, sink, s.State, ws, hclog.NewNullLogger())
	require.NoError(t, err)

	// Run in background
	go func() {
		mSink.Run()
	}()

	// Publish an event
	broker, err := s.State().EventBroker()
	require.NoError(t, err)

	broker.Publish(&structs.Events{Index: 1, Events: []structs.Event{{Topic: "Deployment"}}})

	// Ensure the webhook destination receives event
	select {
	case <-received:
		// pass
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for webhook received")
	}
}

func TestManagedSink_Run_Webhook_Update(t *testing.T) {
	t.Parallel()

	// Setup webhook destination
	received1 := make(chan int, 3)
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var event structs.Events
		dec := json.NewDecoder(r.Body)
		require.NoError(t, dec.Decode(&event))
		require.Equal(t, "Deployment", string(event.Events[0].Topic))

		received1 <- int(event.Index)
	}))
	defer ts1.Close()

	received2 := make(chan int, 3)
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var event structs.Events
		dec := json.NewDecoder(r.Body)
		require.NoError(t, dec.Decode(&event))
		require.Equal(t, "Deployment", string(event.Events[0].Topic))

		received2 <- int(event.Index)
	}))
	defer ts2.Close()

	s, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()
	testutil.WaitForLeader(t, s.RPC)

	s1 := mock.EventSink()
	s1.Address = ts1.URL
	require.NoError(t, s.State().UpsertEventSink(1000, s1))

	ws := memdb.NewWatchSet()
	sink, err := s.State().EventSinkByID(ws, s1.ID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mSink, err := NewManagedSink(ctx, sink, s.State, ws, hclog.NewNullLogger())
	require.NoError(t, err)

	go func() {
		mSink.Run()
	}()

	broker, err := s.State().EventBroker()
	require.NoError(t, err)

	broker.Publish(&structs.Events{Index: 1, Events: []structs.Event{{Topic: "Deployment"}}})

	select {
	case got := <-received1:
		require.Equal(t, 1, got)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for webhook received")
	}

	testutil.WaitForResult(func() (bool, error) {
		ls := atomic.LoadUint64(&mSink.LastSuccess)
		return int(ls) == 1, fmt.Errorf("expected last success to update")
	}, func(err error) {
		require.NoError(t, err)
	})

	// Update sink to point to new address
	s1.Address = ts2.URL
	require.NoError(t, s.State().UpsertEventSink(1001, s1))

	// Wait for the address to propogate
	testutil.WaitForResult(func() (bool, error) {
		return mSink.Sink.Address == s1.Address, fmt.Errorf("expected managed sink address to update")
	}, func(err error) {
		require.Fail(t, err.Error())
	})

	// Publish a new event
	broker.Publish(&structs.Events{Index: 2, Events: []structs.Event{{Topic: "Deployment"}}})

	testutil.WaitForResult(func() (bool, error) {
		ls := atomic.LoadUint64(&mSink.LastSuccess)
		return int(ls) == 2, fmt.Errorf("expected last success to update")
	}, func(err error) {
		require.NoError(t, err)
	})

	// We persist last success so
	select {
	case got := <-received2:
		require.Equal(t, 1, got)
		// pass
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for webhook received")
	}

	// Point back to original
	s1.Address = ts1.URL
	require.NoError(t, s.State().UpsertEventSink(1002, s1))

	testutil.WaitForResult(func() (bool, error) {
		return mSink.Sink.Address == s1.Address, fmt.Errorf("expected managed sink address to update")
	}, func(err error) {
		require.FailNow(t, err.Error())
	})

	broker.Publish(&structs.Events{Index: 3, Events: []structs.Event{{Topic: "Deployment"}}})
	select {
	case got := <-received1:
		got2 := <-received1
		//
		require.Equal(t, 2, got)
		require.Equal(t, 3, got2)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for webhook received")
	}
}

func TestManagedSink_Shutdown(t *testing.T) {
	t.Parallel()

	s, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()

	s1 := mock.EventSink()
	require.NoError(t, s.State().UpsertEventSink(1000, s1))

	ws := memdb.NewWatchSet()
	sink, err := s.State().EventSinkByID(ws, s1.ID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create sink
	mSink, err := NewManagedSink(ctx, sink, s.State, ws, hclog.NewNullLogger())
	require.NoError(t, err)

	// Run in background
	closed := make(chan struct{})
	go func() {
		err := mSink.Run()
		close(closed)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	}()

	// Stop the parent context
	cancel()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		require.Fail(t, "expected managed sink to stop")
	}
}

func TestManagedSink_DeregisterSink(t *testing.T) {
	t.Parallel()

	s, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()

	s1 := mock.EventSink()
	require.NoError(t, s.State().UpsertEventSink(1000, s1))

	ws := memdb.NewWatchSet()
	sink, err := s.State().EventSinkByID(ws, s1.ID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create sink
	mSink, err := NewManagedSink(ctx, sink, s.State, ws, hclog.NewNullLogger())
	require.NoError(t, err)

	// Run in background
	closed := make(chan struct{})
	go func() {
		err := mSink.Run()
		close(closed)
		require.Error(t, err)
		require.Equal(t, ErrEventSinkDeregistered, err)
	}()

	// Stop the parent context
	require.NoError(t, s.State().DeleteEventSinks(1001, []string{s1.ID}))

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		require.Fail(t, "expected managed sink to stop")
	}
}
