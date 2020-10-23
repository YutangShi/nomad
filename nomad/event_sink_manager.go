package nomad

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
)

type Sink interface {
	Start() error
	Stop()
	Subscribe() error
}

type SinkWriter interface {
	Send(ctx context.Context, e *structs.Events) error
}

type Manager struct {
	ctx context.Context

	broker *stream.EventBroker

	sinkSubscriptions map[string]*ManagedSink
	updateCh          chan SinkUpdate
}

type SinkUpdate struct {
	ID    string
	Error error
}

type ManagedSink struct {
	Sink         *structs.EventSink
	WatchCh      <-chan error
	Subscription *stream.Subscription
	SinkWriter   SinkWriter
}

func NewManager(ctx context.Context, sinks []string, s *state.StateStore) (*Manager, error) {
	broker, err := s.EventBroker()
	if err != nil {
		// TODO drew, this cancel smells
		return nil, err
	}

	m := &Manager{
		broker:            broker,
		ctx:               ctx,
		sinkSubscriptions: make(map[string]*ManagedSink),
		updateCh:          make(chan SinkUpdate),
	}

	// Set up an initial watchset for each sink
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

		// Create the writer for the sink
		writer, err := stream.NewWebhookSinks(sink)
		if err != nil {
			return nil, fmt.Errorf("generating sink writer for sink %w", err)
		}
		wsCh := ws.WatchCh(ctx)

		mSink := &ManagedSink{
			Sink:       sink,
			WatchCh:    wsCh,
			SinkWriter: writer,
		}
		m.sinkSubscriptions[sink.ID] = mSink
	}

	// Notify updatedCh when a watchset for a given sink is triggered
	for id, v := range m.sinkSubscriptions {
		go func(id string, wsCh <-chan error) {
			err := <-wsCh
			m.updateCh <- SinkUpdate{ID: id, Error: err}
		}(id, v.WatchCh)
	}

	return m, nil
}

func (m *Manager) UpdateCh() <-chan SinkUpdate {
	return m.updateCh
}

func (m *Manager) SubscribeAll() error {
	for _, ms := range m.sinkSubscriptions {
		req := &stream.SubscribeRequest{
			Topics: ms.Sink.Topics,
		}
		sub, err := m.broker.Subscribe(req)
		if err != nil {
			return fmt.Errorf("unable to subscribe sink %w", err)
		}
		ms.Subscription = sub
	}

	return nil
}

func (m *ManagedSink) Run(ctx context.Context) error {
	// Activate subscriptions
	if m.Subscription == nil {
		return fmt.Errorf("invalid subscription")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, err := m.Subscription.Next(ctx)
		if err != nil {
			if err == stream.ErrSubscriptionClosed {
				// TODO drew resub
			}
			return err
		}

		err = m.SinkWriter.Send(ctx, &events)
		if err != nil {
			return err
		}
	}
}
