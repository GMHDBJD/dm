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

package master

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewHandleErrorCmd creates a HandleError command
func NewHandleErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "handle-error <operate-type> [-s source ...] <task-name> [-b binlog-pos] [replace-sql1;replace-sql2;]",
		Short: "skip/replace the current error event or a specific binlog position (binlog-pos) event",
		Run:   handleErrorFunc,
	}
	cmd.Flags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the handler-error operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	return cmd
}

func convertOperation(t string) pb.HandleOp {
	switch t {
	case "skip":
		return pb.HandleOp_SkipError
	case "replace":
		return pb.HandleOp_ReplaceError
	default:
		return pb.HandleOp_InvalidHandleOp
	}
}

// handleErrorFunc does handle error request
func handleErrorFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	operation := cmd.Flags().Arg(0)
	taskName := cmd.Flags().Arg(1)
	var sqls []string

	op := convertOperation(operation)
	switch op {
	case pb.HandleOp_InvalidHandleOp:
		common.PrintLines("invalid operation '%s', please use `skip` or `relpace`", operation)
		return
	case pb.HandleOp_ReplaceError:
		if len(cmd.Flags().Args()) <= 2 {
			common.PrintLines("must specify the replace-sqls for replace operation")
			return
		}

		var err error
		sqls, err = common.ExtractSQLsFromArgs(cmd.Flags().Args()[2:])
		if err != nil {
			common.PrintLines("%v", err)
			return
		}
	default:
		if len(cmd.Flags().Args()) > 2 {
			common.PrintLines("replace-sqls will be ignored for 'replace' operation")
		}
	}

	binlogPos, err := cmd.Flags().GetString("binlog-pos")
	if err != nil {
		common.PrintLines("%v", err)
		return
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()

	resp, err := cli.HandleError(ctx, &pb.HandleErrorRequest{
		Op:        pb.HandleOp_SkipError,
		Task:      taskName,
		BinlogPos: binlogPos,
		Sqls:      sqls,
		Sources:   sources,
	})
	if err != nil {
		common.PrintLines("can not handle error:\n%v", err)
		return
	}

	common.PrettyPrintResponse(resp)
}
