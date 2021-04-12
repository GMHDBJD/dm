// Copyright 2019 PingCAP, Inc.
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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"bytes"
	"encoding/binary"

	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// flags used in RowsEvent.
const (
	RowFlagsEndOfStatement     uint16 = 0x0001
	RowFlagsNoForeignKeyChecks uint16 = 0x0002
	RowFlagsNoUniqueKeyChecks  uint16 = 0x0004
	RowFlagsRowHasAColumns     uint16 = 0x0008
)

const (
	// GTIDFlagsCommitYes represents a GTID flag with [commit=yes].
	// in `Row` binlog format, this will appear in GTID event before DDL query event.
	GTIDFlagsCommitYes uint8 = 1

	binlogVersion   uint16 = 4 // only binlog-version 4 supported now
	mysqlVersion           = "5.7.22-log"
	mysqlVersionLen        = 50                                 // fix-length
	eventHeaderLen         = uint8(replication.EventHeaderSize) // always 19
	crc32Len        uint32 = 4                                  // CRC32-length
	tableMapFlags   uint16 = 1                                  // flags in TableMapEvent's post-header, not used yet

	// MinUserVarEventLen represents the minimum event length for a USER_VAR_EVENT with checksum
	MinUserVarEventLen = uint32(eventHeaderLen+4+1+1) + crc32Len // 29 bytes
	// MinQueryEventLen represents the minimum event length for a QueryEvent with checksum
	MinQueryEventLen = uint32(eventHeaderLen+4+4+1+2+2+1+1) + crc32Len // 38 bytes
)

var (
	// A array indexed by `Binlog-Event-Type - 1` to extract the length of the event specific header.
	// It is copied from a binlog file generated by MySQL 5.7.22-log.
	// The doc at https://dev.mysql.com/doc/internals/en/format-description-event.html does not include all of them.
	eventTypeHeaderLen = []byte{
		0x38, 0x0d, 0x00, 0x08, 0x00, 0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x5f, 0x00,
		0x04, 0x1a, 0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a, 0x0a,
		0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00,
	}
	// user var name used in dummy USER_VAR_EVENT
	dummyUserVarName = []byte("!dummyvar")
	// dummy (commented) query in a QueryEvent
	dummyQuery = []byte("# dummy query generated by DM, often used to fill a hole in a binlog file")
)

// GenEventHeader generates a EventHeader's raw data according to a passed-in EventHeader struct.
// ref: https://dev.mysql.com/doc/internals/en/binlog-event-header.html
func GenEventHeader(header *replication.EventHeader) ([]byte, error) {
	buf := new(bytes.Buffer)

	// timestamp, 4 bytes
	err := binary.Write(buf, binary.LittleEndian, header.Timestamp)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write timestamp %d", header.Timestamp)
	}

	// event_type, 1 byte
	err = binary.Write(buf, binary.LittleEndian, header.EventType)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event_type %v", header.EventType)
	}

	// server_id, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, header.ServerID)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write server_id %d", header.ServerID)
	}

	// event_size, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, header.EventSize)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event_size %d", header.EventSize)
	}

	// log_pos, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, header.LogPos)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write log_pos %d", header.LogPos)
	}

	// flags, 2 bytes
	err = binary.Write(buf, binary.LittleEndian, header.Flags)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write flags % X", header.Flags)
	}

	// try to decode the data
	eh := replication.EventHeader{}
	err = eh.Decode(buf.Bytes())
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "decode % X", buf.Bytes())
	}

	return buf.Bytes(), nil
}

// GenFormatDescriptionEvent generates a FormatDescriptionEvent.
// ref: https://dev.mysql.com/doc/internals/en/format-description-event.html.
func GenFormatDescriptionEvent(header *replication.EventHeader, latestPos uint32) (*replication.BinlogEvent, error) {
	payload := new(bytes.Buffer)

	// binlog-version, 2 bytes
	err := binary.Write(payload, binary.LittleEndian, binlogVersion)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write binlog-version %d", binlogVersion)
	}

	// mysql-server version, 50 bytes
	serverVer := make([]byte, mysqlVersionLen)
	copy(serverVer, mysqlVersion)
	err = binary.Write(payload, binary.LittleEndian, serverVer)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write mysql-server version %v", serverVer)
	}

	// create_timestamp, 4 bytes
	err = binary.Write(payload, binary.LittleEndian, header.Timestamp)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write create_timestamp %d", header.Timestamp)
	}

	// event_header_length, 1 byte
	err = binary.Write(payload, binary.LittleEndian, eventHeaderLen)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event_header_length %d", eventHeaderLen)
	}

	// event type header length, 38 bytes now
	err = binary.Write(payload, binary.LittleEndian, eventTypeHeaderLen)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event type header length % X", eventTypeHeaderLen)
	}

	// checksum algorithm, 1 byte
	err = binary.Write(payload, binary.LittleEndian, replication.BINLOG_CHECKSUM_ALG_CRC32)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write checksum algorithm % X", replication.BINLOG_CHECKSUM_ALG_CRC32)
	}

	buf := new(bytes.Buffer)
	event := &replication.FormatDescriptionEvent{}
	ev, err := assembleEvent(buf, event, true, *header, replication.FORMAT_DESCRIPTION_EVENT, latestPos, nil, payload.Bytes())
	return ev, err
}

// GenRotateEvent generates a RotateEvent.
// ref: https://dev.mysql.com/doc/internals/en/rotate-event.html
func GenRotateEvent(header *replication.EventHeader, latestPos uint32, nextLogName []byte, position uint64) (*replication.BinlogEvent, error) {
	if len(nextLogName) == 0 {
		return nil, terror.ErrBinlogEmptyNextBinName.Generate()
	}

	// Post-header
	postHeader := new(bytes.Buffer)
	err := binary.Write(postHeader, binary.LittleEndian, position)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write position %d", position)
	}

	// Payload
	payload := new(bytes.Buffer)
	err = binary.Write(payload, binary.LittleEndian, nextLogName)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write next binlog name % X", nextLogName)
	}

	buf := new(bytes.Buffer)
	event := &replication.RotateEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.ROTATE_EVENT, latestPos, postHeader.Bytes(), payload.Bytes())
	return ev, err
}

// GenPreviousGTIDsEvent generates a PreviousGTIDsEvent.
// MySQL has no internal doc for PREVIOUS_GTIDS_EVENT.
// we ref:
//   a. https://github.com/vitessio/vitess/blob/28e7e5503a6c3d3b18d4925d95f23ebcb6f25c8e/go/mysql/binlog_event_mysql56.go#L56
//   b. https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func GenPreviousGTIDsEvent(header *replication.EventHeader, latestPos uint32, gSet gtid.Set) (*replication.BinlogEvent, error) {
	if gSet == nil {
		return nil, terror.ErrBinlogEmptyGTID.Generate()
	}

	origin := gSet.Origin()
	if origin == nil {
		return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(gSet.String())
	}

	// event payload, GTID set encoded in it
	payload := origin.Encode()

	buf := new(bytes.Buffer)
	event := &replication.PreviousGTIDsEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.PREVIOUS_GTIDS_EVENT, latestPos, nil, payload)
	return ev, err
}

// GenGTIDEvent generates a GTIDEvent.
// MySQL has no internal doc for GTID_EVENT.
// we ref the `GTIDEvent.Decode` in go-mysql.
// `uuid` is the UUID part of the GTID, like `9f61c5f9-1eef-11e9-b6cf-0242ac140003`.
// `gno` is the GNO part of the GTID, like `6`.
func GenGTIDEvent(header *replication.EventHeader, latestPos uint32, gtidFlags uint8, uuid string, gno int64, lastCommitted int64, sequenceNumber int64) (*replication.BinlogEvent, error) {
	payload := new(bytes.Buffer)

	// GTID flags, 1 byte
	err := binary.Write(payload, binary.LittleEndian, gtidFlags)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write GTID flags % X", gtidFlags)
	}

	// SID, 16 bytes
	sid, err := ParseSID(uuid)
	if err != nil {
		return nil, terror.Annotatef(err, "parse UUID %s to SID", uuid)
	}
	err = binary.Write(payload, binary.LittleEndian, sid.Bytes())
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write SID % X", sid.Bytes())
	}

	// GNO, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, gno)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write GNO %d", gno)
	}

	// length of TypeCode, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(replication.LogicalTimestampTypeCode))
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write length of TypeCode %d", replication.LogicalTimestampTypeCode)
	}

	// lastCommitted, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, lastCommitted)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write last committed sequence number %d", lastCommitted)
	}

	// sequenceNumber, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, sequenceNumber)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write sequence number %d", sequenceNumber)
	}

	buf := new(bytes.Buffer)
	event := &replication.GTIDEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.GTID_EVENT, latestPos, nil, payload.Bytes())
	return ev, err
}

// GenQueryEvent generates a QueryEvent.
// ref: https://dev.mysql.com/doc/internals/en/query-event.html
// ref: http://blog.51cto.com/yanzongshuai/2087782
// `statusVars` should be generated out of this function, we can implement it later.
// `len(query)` must > 0.
func GenQueryEvent(header *replication.EventHeader, latestPos uint32, slaveProxyID uint32, executionTime uint32, errorCode uint16, statusVars []byte, schema []byte, query []byte) (*replication.BinlogEvent, error) {
	if len(query) == 0 {
		return nil, terror.ErrBinlogEmptyQuery.Generate()
	}

	// Post-header
	postHeader := new(bytes.Buffer)

	// slave_proxy_id (thread_id), 4 bytes
	err := binary.Write(postHeader, binary.LittleEndian, slaveProxyID)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write slave_proxy_id %d", slaveProxyID)
	}

	// executionTime, 4 bytes
	err = binary.Write(postHeader, binary.LittleEndian, executionTime)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write execution %d", executionTime)
	}

	// schema length, 1 byte
	schemaLength := uint8(len(schema))
	err = binary.Write(postHeader, binary.LittleEndian, schemaLength)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write schema length %d", schemaLength)
	}

	// error code, 2 bytes
	err = binary.Write(postHeader, binary.LittleEndian, errorCode)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write error code %d", errorCode)
	}

	// status-vars length, 2 bytes
	statusVarsLength := uint16(len(statusVars))
	err = binary.Write(postHeader, binary.LittleEndian, statusVarsLength)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write status-vars length %d", statusVarsLength)
	}

	// Payload
	payload := new(bytes.Buffer)

	// status-vars, status-vars length bytes
	if statusVarsLength > 0 {
		err = binary.Write(payload, binary.LittleEndian, statusVars)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write status-vars % X", statusVars)
		}
	}

	// schema, schema length bytes
	if schemaLength > 0 {
		err = binary.Write(payload, binary.LittleEndian, schema)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write schema % X", schema)
		}
	}

	// 0x00, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(0x00))
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write 0x00")
	}

	// query, len(query) bytes
	err = binary.Write(payload, binary.LittleEndian, query)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write query % X", query)
	}

	buf := new(bytes.Buffer)
	event := &replication.QueryEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.QUERY_EVENT, latestPos, postHeader.Bytes(), payload.Bytes())
	return ev, err
}

// GenTableMapEvent generates a TableMapEvent.
// ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
// ref: https://dev.mysql.com/doc/internals/en/describing-packets.html#type-lenenc_int
// ref: http://blog.51cto.com/yanzongshuai/2090758
// `len(schema)` must > 0, `len(table)` must > 0, `len(columnType)` must > 0.
// `columnType` should be generated out of this function, we can implement it later.
func GenTableMapEvent(header *replication.EventHeader, latestPos uint32, tableID uint64, schema []byte, table []byte, columnType []byte) (*replication.BinlogEvent, error) {
	if len(schema) == 0 || len(table) == 0 || len(columnType) == 0 {
		return nil, terror.ErrBinlogTableMapEvNotValid.Generate(schema, table, columnType)
	}

	// Post-header
	postHeader := new(bytes.Buffer)

	tableIDSize := 6 // for binlog V4, this should be 6.
	if eventTypeHeaderLen[replication.TABLE_MAP_EVENT-1] == 6 {
		tableIDSize = 4
	}

	// table id, tableIDSize bytes
	err := binary.Write(postHeader, binary.LittleEndian, tableID)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write table id %d", tableID)
	}
	postHeader.Truncate(tableIDSize) // truncate the unwanted bytes

	// flags, 2 bytes
	err = binary.Write(postHeader, binary.LittleEndian, tableMapFlags)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write flags %d", tableMapFlags)
	}

	// Payload
	payload := new(bytes.Buffer)

	// schema name length, 1 byte
	schemaLen := uint8(len(schema))
	err = binary.Write(payload, binary.LittleEndian, schemaLen)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write schema name length %d", schemaLen)
	}

	// schema name, schema name length bytes
	err = binary.Write(payload, binary.LittleEndian, schema)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write schema name % X", schema)
	}

	// 0x00, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(0x00))
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write 0x00")
	}

	// table name length, 1 byte
	tableLen := uint8(len(table))
	err = binary.Write(payload, binary.LittleEndian, tableLen)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write table name length %d", tableLen)
	}

	// table name, table name length bytes
	err = binary.Write(payload, binary.LittleEndian, table)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write table name % X", table)
	}

	// 0x00, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(0x00))
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write 0x00")
	}

	// column-count, lenenc-int
	columnCount := gmysql.PutLengthEncodedInt(uint64(len(columnType)))
	err = binary.Write(payload, binary.LittleEndian, columnCount)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write column-count % X", columnCount)
	}

	// column-type-def, column-count bytes
	err = binary.Write(payload, binary.LittleEndian, columnType)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write column-type-def % X", columnType)
	}

	// column-meta-def, lenenc-str
	columnMeta, err := encodeTableMapColumnMeta(columnType)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "generate column-meta-def for column-type-def % X", columnType)
	}
	err = binary.Write(payload, binary.LittleEndian, columnMeta)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write column-meta-def % X", columnMeta)
	}

	// NULL-bitmask, (column-count + 8) / 7 bytes
	bitMaskLen := bitmapByteSize(len(columnType))
	bitMask := nullBytes(bitMaskLen)
	err = binary.Write(payload, binary.LittleEndian, bitMask)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write NULL-bitmask % X", bitMask)
	}

	buf := new(bytes.Buffer)
	_, err = assembleEvent(buf, nil, false, *header, replication.TABLE_MAP_EVENT, latestPos, postHeader.Bytes(), payload.Bytes())
	if err != nil {
		return nil, terror.Annotate(err, "combine event data")
	}

	// sad, in order to Decode a TableMapEvent, we need to set `tableIDSize` first, but it's a private field.
	// so, we need to use a BinlogParser to parse a FormatDescriptionEvent first.
	formatDescEv, err := GenFormatDescriptionEvent(header, 4)
	if err != nil {
		return nil, terror.Annotate(err, "generate FormatDescriptionEvent")
	}

	var tableMapEvent *replication.BinlogEvent
	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			if e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
				return terror.ErrBinlogExpectFormatDescEv.Generate(e)
			}
		case 2: // TableMapEvent
			if e.Header.EventType != replication.TABLE_MAP_EVENT {
				return terror.ErrBinlogExpectTableMapEv.Generate(e)
			}
			tableMapEvent = e
		default:
			return terror.ErrBinlogUnexpectedEv.Generate(e)
		}
		return nil
	}

	parse2 := replication.NewBinlogParser()
	parse2.SetVerifyChecksum(true)
	// parse FormatDescriptionEvent
	_, err = parse2.ParseSingleEvent(bytes.NewReader(formatDescEv.RawData), onEventFunc)
	if err != nil {
		return nil, terror.ErrBinlogParseSingleEv.AnnotateDelegate(err, "parse FormatDescriptionEvent % X", formatDescEv.RawData)
	}

	// parse TableMapEvent
	_, err = parse2.ParseSingleEvent(bytes.NewReader(buf.Bytes()), onEventFunc)
	if err != nil {
		return nil, terror.ErrBinlogParseSingleEv.AnnotateDelegate(err, "parse TableMapEvent % X", buf.Bytes())
	}

	return tableMapEvent, nil
}

// GenRowsEvent generates a RowsEvent.
// RowsEvent includes:
//   WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2
//   UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2
//   DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2
// ref: https://dev.mysql.com/doc/internals/en/rows-event.html
// ref: http://blog.51cto.com/yanzongshuai/2090894
func GenRowsEvent(header *replication.EventHeader, latestPos uint32, eventType replication.EventType, tableID uint64, rowsFlags uint16, rows [][]interface{}, columnType []byte, tableMapEv *replication.BinlogEvent) (*replication.BinlogEvent, error) {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
	default:
		return nil, terror.ErrBinlogEventTypeNotValid.Generate(eventType)
	}

	if len(rows) == 0 {
		return nil, terror.ErrBinlogEventNoRows.Generate()
	}
	if len(columnType) == 0 {
		return nil, terror.ErrBinlogEventNoColumns.Generate()
	}
	for _, row := range rows {
		if len(row) != len(columnType) {
			// all rows have the same length (no nil), and equal to the length of column-type
			return nil, terror.ErrBinlogEventRowLengthNotEq.Generate(len(row), len(columnType))
		}
	}

	postHeader := new(bytes.Buffer)

	tableIDSize := 6
	if eventTypeHeaderLen[eventType] == 6 {
		tableIDSize = 4
	}
	// table id, tableIDSize bytes
	err := binary.Write(postHeader, binary.LittleEndian, tableID)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write table id %d", tableID)
	}
	postHeader.Truncate(tableIDSize) // truncate the unwanted bytes

	// flags, 2 bytes
	err = binary.Write(postHeader, binary.LittleEndian, rowsFlags)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write flags %d", rowsFlags)
	}

	// extra-data
	switch eventType {
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		// if version=2, extra data exist.
		// NOTE: we do not support to write any meaningful extra data yet.
		var extraDataLen uint16 = 2 // two bytes, but with value `2` (no extra data, only this len variable)
		err = binary.Write(postHeader, binary.LittleEndian, extraDataLen)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write extra data length %d", extraDataLen)
		}
	default:
	}

	payload := new(bytes.Buffer)

	// number of columns, lenenc-int
	columnCount := gmysql.PutLengthEncodedInt(uint64(len(rows[0])))
	err = binary.Write(payload, binary.LittleEndian, columnCount)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write number of columns % X", columnCount)
	}

	// columns-present-bitmap1, (num of columns+7)/8 bytes
	byteCount := bitmapByteSize(len(rows[0]))
	bitmap := fullBytes(byteCount) // NOTE: only support to write full columns now
	err = binary.Write(payload, binary.LittleEndian, bitmap)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write columns-present-bitmap1 % X", bitmap)
	}

	isUpdateV1V2 := eventType == replication.UPDATE_ROWS_EVENTv1 || eventType == replication.UPDATE_ROWS_EVENTv2
	if isUpdateV1V2 {
		// NOTE: use columns-present-bitmap1 as columns-present-bitmap2 now
		err = binary.Write(payload, binary.LittleEndian, bitmap)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write columns-present-bitmap2 % X", bitmap)
		}
	}

	columnMetaData, err := encodeTableMapColumnMeta(columnType)
	if err != nil {
		return nil, terror.Annotatef(err, "encode column-meta-def from column-type % X", columnType)
	}
	columnMeta, err := decodeTableMapColumnMeta(columnMetaData, columnType)
	if err != nil {
		return nil, terror.Annotatef(err, "decode column-meta-def %X", columnMetaData)
	}

	// for UPDATE, two rows for one statement
	// currently, columns-present-bitmap2 is just columns-present-bitmap1
	for _, row := range rows {
		// nul-bitmap, (num of columns+7)/8 bytes
		nulBitmap := nullBytes(byteCount) // NOTE: no column with nil value now
		err = binary.Write(payload, binary.LittleEndian, nulBitmap)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write nul-bitmap % X for row %v", nulBitmap, row)
		}

		for i, col := range row {
			var colData []byte
			colData, err = encodeColumnValue(col, columnType[i], columnMeta[i])
			if err != nil {
				return nil, terror.Annotatef(err, "encode column value %v to bytes", col)
			}
			err = binary.Write(payload, binary.LittleEndian, colData)
			if err != nil {
				return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write column data % X", colData)
			}
		}
	}

	buf := new(bytes.Buffer)
	_, err = assembleEvent(buf, nil, false, *header, eventType, latestPos, postHeader.Bytes(), payload.Bytes())
	if err != nil {
		return nil, err
	}

	// in order to Decode RowsEvent, we need to set `tableIDSize` and `tables` first, but they are private fields.
	// so we should parse a FormatDescriptionEvent and a TableMapEvent first.
	var rowsEvent *replication.BinlogEvent
	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			if e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
				return terror.ErrBinlogExpectFormatDescEv.Generate(e)
			}
		case 2: // TableMapEvent
			if e.Header.EventType != replication.TABLE_MAP_EVENT {
				return terror.ErrBinlogExpectTableMapEv.Generate(e)
			}
		case 3: // RowsEvent
			if e.Header.EventType != eventType {
				return terror.ErrBinlogExpectRowsEv.Generate(eventType, e)
			}
			rowsEvent = e
		default:
			return terror.ErrBinlogUnexpectedEv.Generate(e)
		}
		return nil
	}

	parse2 := replication.NewBinlogParser()
	parse2.SetVerifyChecksum(true)

	// parse FormatDescriptionEvent
	formatDescEv, err := GenFormatDescriptionEvent(header, 4)
	if err != nil {
		return nil, terror.Annotate(err, "generate FormatDescriptionEvent")
	}
	_, err = parse2.ParseSingleEvent(bytes.NewReader(formatDescEv.RawData), onEventFunc)
	if err != nil {
		return nil, terror.ErrBinlogParseSingleEv.AnnotateDelegate(err, "parse FormatDescriptionEvent % X", formatDescEv.RawData)
	}

	// parse TableMapEvent
	if tableMapEv == nil {
		tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, []byte("schema-placeholder"), []byte("table-placeholder"), columnType)
		if err != nil {
			return nil, terror.Annotate(err, "generate TableMapEvent")
		}
	}
	_, err = parse2.ParseSingleEvent(bytes.NewReader(tableMapEv.RawData), onEventFunc)
	if err != nil {
		return nil, terror.ErrBinlogParseSingleEv.AnnotateDelegate(err, "parse TableMapEvent % x", tableMapEv.RawData)
	}

	// parse RowsEvent
	_, err = parse2.ParseSingleEvent(bytes.NewReader(buf.Bytes()), onEventFunc)
	if err != nil {
		return nil, terror.ErrBinlogParseSingleEv.AnnotateDelegate(err, "parse RowsEvent % X", buf.Bytes())
	}

	return rowsEvent, nil
}

// GenXIDEvent generates a XIDEvent.
// ref: https://dev.mysql.com/doc/internals/en/xid-event.html
func GenXIDEvent(header *replication.EventHeader, latestPos uint32, xid uint64) (*replication.BinlogEvent, error) {
	// Payload
	payload := new(bytes.Buffer)
	err := binary.Write(payload, binary.LittleEndian, xid)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write XID %d", xid)
	}

	buf := new(bytes.Buffer)
	event := &replication.XIDEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.XID_EVENT, latestPos, nil, payload.Bytes())
	return ev, err
}

// GenMariaDBGTIDListEvent generates a MariadbGTIDListEvent.
// ref: https://mariadb.com/kb/en/library/gtid_list_event/
func GenMariaDBGTIDListEvent(header *replication.EventHeader, latestPos uint32, gSet gtid.Set) (*replication.BinlogEvent, error) {
	if gSet == nil || len(gSet.String()) == 0 {
		return nil, terror.ErrBinlogEmptyGTID.Generate()
	}

	origin := gSet.Origin()
	if origin == nil {
		return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(gSet.String())
	}
	mariaDBGSet, ok := origin.(*gmysql.MariadbGTIDSet)
	if !ok {
		return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(gSet.String())
	}

	payload := new(bytes.Buffer)

	// Number of GTIDs, 4 bytes
	numOfGTIDs := uint32(len(mariaDBGSet.Sets))
	err := binary.Write(payload, binary.LittleEndian, numOfGTIDs)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write Number of GTIDs %d", numOfGTIDs)
	}

	for _, mGTID := range mariaDBGSet.Sets {
		// Replication Domain ID, 4 bytes
		err = binary.Write(payload, binary.LittleEndian, mGTID.DomainID)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write Replication Domain ID %d", mGTID.DomainID)
		}
		// Server_ID, 4 bytes
		err = binary.Write(payload, binary.LittleEndian, mGTID.ServerID)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write Server_ID %d", mGTID.ServerID)
		}
		// GTID sequence, 8 bytes
		err = binary.Write(payload, binary.LittleEndian, mGTID.SequenceNumber)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write GTID sequence %d", mGTID.SequenceNumber)
		}
	}

	buf := new(bytes.Buffer)
	event := &replication.MariadbGTIDListEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.MARIADB_GTID_LIST_EVENT, latestPos, nil, payload.Bytes())
	return ev, err
}

// GenMariaDBGTIDEvent generates a MariadbGTIDEvent.
// ref: https://mariadb.com/kb/en/library/gtid_event/
func GenMariaDBGTIDEvent(header *replication.EventHeader, latestPos uint32, sequenceNum uint64, domainID uint32) (*replication.BinlogEvent, error) {
	payload := new(bytes.Buffer)

	// GTID sequence, 8 bytes
	err := binary.Write(payload, binary.LittleEndian, sequenceNum)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write GTID sequence %d", sequenceNum)
	}

	// Replication Domain ID, 4 bytes
	err = binary.Write(payload, binary.LittleEndian, domainID)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write Replication Domain ID %d", domainID)
	}

	// Flags, 1 byte, keep zero now.
	xidFlags := uint8(0x00)
	err = binary.Write(payload, binary.LittleEndian, xidFlags)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write Flags %d", xidFlags)
	}

	// "if flag & FL_GROUP_COMMIT_ID" is FALSE
	// commit_id, 6 bytes with zero value
	err = binary.Write(payload, binary.LittleEndian, uint64(0x00))
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write 6 bytes commit_id with zero value")
	}
	payload.Truncate(payload.Len() - 2) // len(uint64) - 2 == 6 bytes

	buf := new(bytes.Buffer)
	event := &replication.MariadbGTIDEvent{}
	ev, err := assembleEvent(buf, event, false, *header, replication.MARIADB_GTID_EVENT, latestPos, nil, payload.Bytes())
	return ev, err
}

// GenDummyEvent generates a dummy QueryEvent or a dummy USER_VAR_EVENT.
// Dummy events often used to fill the holes in a relay log file which lacking some events from the master.
// The minimum size is 29 bytes (19 bytes header + 6 bytes body for a USER_VAR_EVENT + 4 bytes checksum).
// ref: https://dev.mysql.com/doc/internals/en/user-var-event.html
// ref: https://github.com/MariaDB/server/blob/a765b19e5ca31a3d866cdbc8bef3a6f4e5e44688/sql/log_event.cc#L4950
func GenDummyEvent(header *replication.EventHeader, latestPos uint32, eventSize uint32) (*replication.BinlogEvent, error) {
	if eventSize < MinUserVarEventLen {
		return nil, terror.ErrBinlogDummyEvSizeTooSmall.Generate(eventSize, MinUserVarEventLen)
	}

	// modify flag in the header
	headerClone := *header // do a copy
	headerClone.Flags &= ^replication.LOG_EVENT_THREAD_SPECIFIC_F
	headerClone.Flags |= replication.LOG_EVENT_SUPPRESS_USE_F
	headerClone.Flags |= replication.LOG_EVENT_RELAY_LOG_F // now, the dummy event created by relay only

	if eventSize < MinQueryEventLen {
		// generate a USER_VAR_EVENT
		var (
			payload   = new(bytes.Buffer)
			buf       = new(bytes.Buffer)
			event     = &replication.GenericEvent{}
			eventType = replication.USER_VAR_EVENT
			nameLen   = eventSize - (MinUserVarEventLen - 1)
			nameBytes = make([]byte, nameLen)
		)
		copy(nameBytes, dummyUserVarName)
		// name_length, 4 bytes
		err := binary.Write(payload, binary.LittleEndian, nameLen)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write USER_VAR_EVENT name length %d", nameLen)
		}
		// name, name_length bytes (now, at least 1 byte)
		err = binary.Write(payload, binary.LittleEndian, nameBytes)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write USER_VAR_EVENT name % X", nameBytes)
		}
		// is_null, 1 byte
		isNull := byte(1) // always is null (no `value` part)
		err = binary.Write(payload, binary.LittleEndian, isNull)
		if err != nil {
			return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write USER_VAR_EVENT is-null % X", isNull)
		}
		ev, err := assembleEvent(buf, event, false, headerClone, eventType, latestPos, nil, payload.Bytes())
		return ev, err
	}

	// generate a QueryEvent
	queryLen := eventSize - (MinQueryEventLen - 1)
	queryBytes := make([]byte, queryLen)
	copy(queryBytes, dummyQuery)
	ev, err := GenQueryEvent(&headerClone, latestPos, 0, 0, 0, nil, nil, queryBytes)
	return ev, err
}

// GenHeartbeatEvent generates a heartbeat event.
// ref: https://dev.mysql.com/doc/internals/en/heartbeat-event.html
func GenHeartbeatEvent(header *replication.EventHeader) *replication.BinlogEvent {
	// modify header
	headerClone := *header // do a copy
	headerClone.Flags = 0
	headerClone.EventSize = 39
	headerClone.Timestamp = 0
	headerClone.EventType = replication.HEARTBEAT_EVENT

	eventBytes := make([]byte, 39)
	ev := &replication.BinlogEvent{Header: &headerClone, Event: &replication.GenericEvent{Data: eventBytes}}

	return ev
}
