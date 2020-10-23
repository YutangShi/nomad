package nomad

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/state"
)

type Sink interface {
	Start() error
	Stop()
	Subscribe() error
}

type Manager struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	sinkChs  map[string]<-chan error
	updateCh chan SinkUpdate
}

type SinkUpdate struct {
	ID    string
	Error error
}

func NewManager(ctx context.Context, sinks []string, s *state.StateStore) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:      ctx,
		cancelFn: cancel,
		sinkChs:  make(map[string]<-chan error),
		updateCh: make(chan SinkUpdate),
	}

	for _, id := range sinks {
		ws := memdb.NewWatchSet()
		ws.Add(s.AbandonCh())

		sink, err := s.EventSinkByID(ws, id)
		if err != nil {
			return nil, fmt.Errorf("querying event sink for manager: %w", err)
		}
		if sink == nil {
			return nil, fmt.Errorf("sink not found %s", id)
		}

		wsCh := ws.WatchCh(ctx)
		m.sinkChs[id] = wsCh
	}

	// Create a goroutine for each sink that sends an update when its watchset
	// is triggered
	for id, ch := range m.sinkChs {
		go func(id string, wsCh <-chan error) {
			err := <-wsCh
			m.updateCh <- SinkUpdate{ID: id, Error: err}
		}(id, ch)
	}

	return m, nil
}

func (m *Manager) UpdateCh() <-chan SinkUpdate {
	return m.updateCh
}

// func (m *Manager) Run() {
// 	for {
// 		select {
// 		case <-m.ctx.Done():
// 			return
// 		}
// 	}
// }
