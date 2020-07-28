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

package syncer

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/dm/dm/command"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	tmysql "github.com/pingcap/parser/mysql"
)

// HandleError handle error for syncer
func (s *Syncer) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) (string, error) {
	var posStr string
	var pos *mysql.Position

	if len(req.BinlogPos) == 0 {
		location := s.getErrLocation()
		if location == nil {
			return fmt.Sprintf("source '%s' has no error", s.cfg.SourceID), nil
		}
		posStr = location.Position.String()
		pos = &location.Position
	} else {
		posStr = req.BinlogPos
		pos, _ = command.VerifyBinlogPos(posStr)
	}

	events := make([]*replication.BinlogEvent, 0)
	var err error
	if req.Op == pb.HandleOp_ReplaceError {
		events, err = s.genEvents(req.Sqls, pos.Pos)
		log.L().Info(fmt.Sprintf("gen events %d: ", len(events)), zap.String("sqls", strings.Join(req.Sqls, ",")))
		if err != nil {
			return "", err
		}
	}

	log.L().Info("get handler", zap.Stringer("op", req.Op))
	s.errHandlerHolder.Set(s.tctx, posStr, req.Op, events)

	return "", nil
}

func (s *Syncer) genEvents(sqls []string, pos uint32) ([]*replication.BinlogEvent, error) {
	events := make([]*replication.BinlogEvent, 0)

	parser2 := parser.New()
	if s.cfg.EnableANSIQuotes {
		parser2.SetSQLMode(tmysql.ModeANSIQuotes)
	}

	for _, sql := range sqls {
		node, err := parser2.ParseOneStmt(sql, "", "")
		if err != nil {
			return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "sql %s", sql)
		}

		switch node.(type) {
		case ast.DDLNode:
			log.L().Info("is ddl node")
			tableNames, err := parserpkg.FetchDDLTableNames("", node)
			if err != nil {
				return nil, err
			}

			schema := tableNames[0].Schema
			log.L().Info(fmt.Sprintf("get schema %s", schema))
			if len(schema) == 0 {
				return nil, terror.ErrSyncerUnitInjectDDLWithoutSchema.Generate(sql)
			}
			events = append(events, genQueryEvent([]byte(schema), []byte(sql), pos))
		default:
			return nil, terror.ErrNotSet.New("get other node")
		}
	}
	return events, nil
}

func genQueryEvent(schema, query []byte, pos uint32) *replication.BinlogEvent {
	header := &replication.EventHeader{
		EventType: replication.QUERY_EVENT,
		EventSize: 0,
		LogPos:    pos,
	}
	queryEvent := &replication.QueryEvent{
		Schema: schema,
		Query:  query,
	}
	e := &replication.BinlogEvent{
		Header: header,
		Event:  queryEvent,
	}
	log.L().Info("in genQueryEvent", zap.String("query", string(query)))
	return e
}

func genRowEvent() {
}
