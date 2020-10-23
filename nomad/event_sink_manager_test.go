package nomad

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/nomad/nomad/mock"
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
