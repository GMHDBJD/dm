// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
)

// handler contains an operation for specified binlog pos
// used by `handle-error`
type handler struct {
	uuid   string // add a UUID, make it more friendly to be traced in log
	op     pb.HandleOp
	events []*replication.BinlogEvent
}

// newHandler creates a new handler with a random UUID
func newHandler(tctx *tcontext.Context, op pb.HandleOp, events []*replication.BinlogEvent) *handler {
	return &handler{
		uuid:   uuid.NewV4().String(),
		op:     op,
		events: events,
	}
}

func (h *handler) String() string {
	events := make([]string, 0)
	for _, e := range h.events {
		buf := new(bytes.Buffer)
		e.Dump(buf)
		events = append(events, buf.String())
	}
	return fmt.Sprintf("uuid: %s, op: %s, events: %s", h.uuid, h.op, strings.Join(events, " "))
}

// HandlerHolder holds error handler
type HandlerHolder struct {
	mu       sync.Mutex
	handlers map[string]*handler
}

// NewHandlerHolder creates a new handlerHolder
func NewHandlerHolder() *HandlerHolder {
	return &HandlerHolder{
		handlers: make(map[string]*handler),
	}
}

// Set sets an handler according request
func (h *HandlerHolder) Set(tctx *tcontext.Context, pos string, op pb.HandleOp, events []*replication.BinlogEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	cur := newHandler(tctx, op, events)
	pre, ok := h.handlers[pos]
	if ok {
		tctx.L().Warn("overwrite operator", zap.Stringer("old handler", pre), zap.Stringer("new handler", cur))
	}
	h.handlers[pos] = cur
	tctx.L().Info("set a new handler", zap.Stringer("new handler", cur))
	return nil
}

// Handle tries to apply handler by pos, returns skip, events, error
func (h *HandlerHolder) Handle(tctx *tcontext.Context, location *binlog.Location) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.L().Info(fmt.Sprintf("in handle suffix %d", location.Suffix))

	key := location.Position.String()
	handler, ok := h.handlers[key]
	if !ok {
		return false
	}

	if location.Suffix == 0 {
		location.Suffix = 1
		return true
	}

	if location.Suffix-1 == len(handler.events) {
		location.Suffix = 0
	}

	return false
}

// GetEvent return binlog event
func (h *HandlerHolder) GetEvent(location *binlog.Location) (*replication.BinlogEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	log.L().Info(fmt.Sprintf("get event suffix %d", location.Suffix))

	key := location.Position.String()
	handler, ok := h.handlers[key]
	if !ok {
		return nil, nil
	}

	if len(handler.events) < location.Suffix {
		return nil, nil
	}

	e := handler.events[location.Suffix-1]
	location.Suffix++
	return e, nil
}
