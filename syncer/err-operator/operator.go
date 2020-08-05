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

package operator

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
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Operator contains an operation for specified binlog pos
// used by `handle-error`
type Operator struct {
	uuid   string // add a UUID, make it more friendly to be traced in log
	op     pb.ErrorOp
	events []*replication.BinlogEvent // endLocation -> events
}

// newOperator creates a new operator with a random UUID
func newOperator(op pb.ErrorOp, events []*replication.BinlogEvent) *Operator {
	return &Operator{
		uuid:   uuid.NewV4().String(),
		op:     op,
		events: events,
	}
}

func (o *Operator) String() string {
	events := make([]string, 0)
	for _, e := range o.events {
		buf := new(bytes.Buffer)
		e.Dump(buf)
		events = append(events, buf.String())
	}
	return fmt.Sprintf("uuid: %s, op: %s, events: %s", o.uuid, o.op, strings.Join(events, " "))
}

// Holder holds error operator
type Holder struct {
	mu        sync.Mutex
	operators map[string]*Operator
}

// NewHolder creates a new Holder
func NewHolder() *Holder {
	return &Holder{
		operators: make(map[string]*Operator),
	}
}

// Set sets an Operator
func (h *Holder) Set(pos string, op pb.ErrorOp, events []*replication.BinlogEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	oper := newOperator(op, events)
	pre, ok := h.operators[pos]
	if ok {
		log.L().Warn("overwrite operator", zap.String("position", pos), zap.Stringer("old operator", pre))
	}
	h.operators[pos] = oper
	log.L().Info("set a new operator", zap.String("position", pos), zap.Stringer("new operator", oper))
	return nil
}

// GetEvent return a replace binlog event
// for example:
//						startLocation			endLocation
// event 1				1000, 0					1010, 0
// event 2				1010, 0					1020, 0	<--	replace it with event a,b,c
// replace event a		1010, 0					1010, 1
// replace event b		1010, 1					1010, 2
// replace event c		1010, 2					1020, 0
// event 3				1020, 0					1030, 0
func (h *Holder) GetEvent(startLocation *binlog.Location) (*replication.BinlogEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := startLocation.Position.String()
	operator, ok := h.operators[key]
	if !ok {
		return nil, nil
	}

	if len(operator.events) <= startLocation.Suffix {
		return nil, terror.ErrSyncerReplaceEvent.New("replace events out of index")
	}

	e := operator.events[startLocation.Suffix]
	buf := new(bytes.Buffer)
	e.Dump(buf)
	log.L().Info("get replace event", zap.Stringer("event", buf))

	return e, nil
}

// Apply tries to apply operation for event by location
func (h *Holder) Apply(startLocation, endLocation *binlog.Location) (bool, pb.ErrorOp) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// only apply the origin event
	if endLocation.Suffix != 0 {
		return false, pb.ErrorOp_InvalidErrorOp
	}

	key := startLocation.Position.String()
	operator, ok := h.operators[key]
	if !ok {
		return false, pb.ErrorOp_InvalidErrorOp
	}

	if operator.op == pb.ErrorOp_Replace {
		if len(operator.events) == 0 {
			// this should not happen
			return false, pb.ErrorOp_InvalidErrorOp
		}

		// set LogPos as start position
		for _, ev := range operator.events {
			ev.Header.LogPos = startLocation.Position.Pos
			if e, ok := ev.Event.(*replication.QueryEvent); ok {
				e.GSet = startLocation.GTIDSet.Origin()
			}
		}

		// set the last replace event as end position
		operator.events[len(operator.events)-1].Header.EventSize = endLocation.Position.Pos - startLocation.Position.Pos
		operator.events[len(operator.events)-1].Header.LogPos = endLocation.Position.Pos
		e := operator.events[len(operator.events)-1]
		if e, ok := e.Event.(*replication.QueryEvent); ok {
			e.GSet = endLocation.GTIDSet.Origin()
		}
	}

	log.L().Info("apply a operator", zap.Stringer("startlocation", startLocation), zap.Stringer("endlocation", endLocation), zap.Stringer("operator", operator))

	return true, operator.op
}