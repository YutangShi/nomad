package nomad

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/oklog/run"
)

var ErrEventSinkDeregistered error = errors.New("sink deregistered")

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
	sinkUpdateCh      chan SinkUpdate
	newSinkWs         memdb.WatchSet
}

type SinkUpdate struct {
	ID    string
	Error error
}

type ManagedSink struct {
	stopCtx      context.Context
	Sink         *structs.EventSink
	WatchCh      <-chan error
	doneReset    chan struct{}
	Subscription *stream.Subscription

	LastSuccess uint64
	SinkWriter  SinkWriter

	stateFn func() *state.StateStore
	broker  *stream.EventBroker

	sinkCtx  context.Context
	cancelFn context.CancelFunc

	l hclog.Logger
}

// TODO why pass in sink ids when we have state
func NewManager(ctx context.Context, sinks []string, s *state.StateStore) (*Manager, error) {
	broker, err := s.EventBroker()
	if err != nil {
		// TODO drew, this cancel smells
		return nil, err
	}

	newSinkWs := memdb.NewWatchSet()
	newSinkWs.Add(s.AbandonCh())
	_, err = s.EventSinks(newSinkWs)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		broker:            broker,
		ctx:               ctx,
		sinkSubscriptions: make(map[string]*ManagedSink),
		sinkUpdateCh:      make(chan SinkUpdate),
		newSinkWs:         newSinkWs,
	}

	// Create sinks and writers for Manger to manage
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
		writer, err := stream.NewWebhookSink(sink)
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

		go mSink.NotifyChange(ctx, m.sinkUpdateCh)
	}

	return m, nil
}

func (m *Manager) UpdateCh() <-chan SinkUpdate {
	return m.sinkUpdateCh
}

func (m *Manager) SubscribeAll() (func(), error) {
	var unsubs []func()
	for _, ms := range m.sinkSubscriptions {
		req := &stream.SubscribeRequest{
			Topics: ms.Sink.Topics,
		}
		sub, err := m.broker.Subscribe(req)
		if err != nil {
			return nil, fmt.Errorf("unable to subscribe sink %w", err)
		}
		unsubs = append(unsubs, sub.Unsubscribe)
		ms.Subscription = sub
	}

	unsub := func() {
		for _, unsub := range unsubs {
			unsub()
		}
	}

	return unsub, nil
}

func (m *Manager) CloseAll() {
	for _, ms := range m.sinkSubscriptions {
		ms.Subscription.Unsubscribe()
	}
}

func (m *ManagedSink) Run() error {
	defer m.Subscription.Unsubscribe()
	exitCh := make(chan struct{})
	defer close(exitCh)

	// Listen for changes to EventSink. If there is a change cancel our local
	// context to stop the subscription and reload with new changes.
	go func() {
		for {
			select {
			case <-exitCh:
				return
			case <-m.stopCtx.Done():
				return
			case err := <-m.watchCh():
				if err != nil {
					return
				}

				// Cancel the subscription scoped context
				m.cancelFn()

				// wait until the reset was done
				select {
				case <-m.stopCtx.Done():
					return
				case <-m.doneReset:
				case <-exitCh:
				}
			}
		}
	}()

LOOP:
	for {
		events, err := m.Subscription.Next(m.sinkCtx)
		if err != nil {
			// Shutting down, exit gracefully
			if m.stopCtx.Err() != nil {
				return nil
			}

			// Reloadable error, reload and restart
			if err == stream.ErrSubscriptionClosed || err == context.Canceled {
				if err := m.Reload(); err != nil {
					return err
				}
				goto LOOP
			}
			return err
		}

		err = m.SinkWriter.Send(m.sinkCtx, &events)
		if err != nil {
			if strings.Contains(err.Error(), context.Canceled.Error()) {
				continue
			}
			m.l.Warn("Failed to send event to sink", "sink", m.Sink.ID, "error", err)
			continue
		}
		// Update the last successful index sent
		atomic.StoreUint64(&m.LastSuccess, events.Index)
	}
}

func (m *ManagedSink) Reload() error {
	// Exit if shutting down
	if err := m.stopCtx.Err(); err != nil {
		return err
	}

	// Unsubscribe incase we haven't yet
	m.Subscription.Unsubscribe()

	// Fetch our updated or changed event sink with a new watchset
	ws := memdb.NewWatchSet()
	ws.Add(m.stateFn().AbandonCh())
	sink, err := m.stateFn().EventSinkByID(ws, m.Sink.ID)
	if err != nil {
		return err
	}

	// Sink has been deleted, stop
	if sink == nil {
		return ErrEventSinkDeregistered
	}

	// Reconfigure the sink writer
	writer, err := stream.NewWebhookSink(sink)
	if err != nil {
		return fmt.Errorf("generating sink writer for sink %w", err)
	}

	// Reset values we are updating
	sinkCtx, cancel := context.WithCancel(m.stopCtx)
	m.sinkCtx = sinkCtx
	m.cancelFn = cancel
	m.SinkWriter = writer
	m.Sink = sink
	m.WatchCh = ws.WatchCh(sinkCtx)

	// Resubscribe
	req := &stream.SubscribeRequest{
		Topics: m.Sink.Topics,
		Index:  atomic.LoadUint64(&m.LastSuccess),
	}

	sub, err := m.broker.Subscribe(req)
	if err != nil {
		return fmt.Errorf("unable to subscribe sink %w", err)
	}
	m.Subscription = sub

	// signal we are done reloading
	m.doneReset <- struct{}{}
	return nil
}

func (m *ManagedSink) watchCh() <-chan error {
	return m.WatchCh
}

func NewManagedSink(ctx context.Context, sink *structs.EventSink, state func() *state.StateStore, ws memdb.WatchSet, l hclog.Logger) (*ManagedSink, error) {
	if l == nil {
		return nil, fmt.Errorf("logger was nil")
	}

	writer, err := stream.NewWebhookSink(sink)
	if err != nil {
		return nil, fmt.Errorf("generating sink writer for sink %w", err)
	}
	broker, err := state().EventBroker()
	if err != nil {
		return nil, err
	}

	sinkCtx, cancel := context.WithCancel(ctx)
	ms := &ManagedSink{
		stopCtx:    ctx,
		Sink:       sink,
		WatchCh:    ws.WatchCh(sinkCtx),
		doneReset:  make(chan struct{}),
		SinkWriter: writer,
		broker:     broker,
		cancelFn:   cancel,
		sinkCtx:    sinkCtx,
		stateFn:    state,
		l:          l,
	}

	req := &stream.SubscribeRequest{
		Topics: ms.Sink.Topics,
	}

	sub, err := ms.broker.Subscribe(req)
	if err != nil {
		return nil, fmt.Errorf("unable to subscribe sink %w", err)
	}
	ms.Subscription = sub

	return ms, nil
}

func (m *ManagedSink) NotifyChange(ctx context.Context, notifyCh chan<- SinkUpdate) {
	select {
	case <-ctx.Done():
		return
	case err := <-m.WatchCh:
		select {
		case <-ctx.Done():
			return
		case notifyCh <- SinkUpdate{ID: m.Sink.ID, Error: err}:
		}
	}
}

func (m *Manager) Run(ctx context.Context) {
	var g run.Group

	for _, ms := range m.sinkSubscriptions {
		g.Add(func() error {
			return ms.Run()
		}, func(err error) {
			spew.Dump(err)
		})
	}

	spew.Dump("before!")
	g.Run()
	spew.Dump("aftaaaaaaaa")
}
