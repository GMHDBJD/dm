// Copyright 2021 PingCAP, Inc.
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

package ha

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// LoadWorker uses to watch load worker events.
type LoadWorker struct {
	Task     string
	Source   string
	Worker   string
	IsDelete bool
}

// GetLoadWorker gets the worker which in load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker-name.
func GetLoadWorker(cli *clientv3.Client, task, sourceID string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.LoadWorkerKeyAdapter.Encode(task, sourceID))
	if err != nil {
		return "", 0, err
	}

	if resp.Count <= 0 {
		return "", resp.Header.Revision, nil
	}

	var worker string
	err = json.Unmarshal(resp.Kvs[0].Value, &worker)

	return worker, resp.Header.Revision, err
}

// GetAllLoadWorker gets all the worker which in load stage.
// k/v: (task, sourceID) -> worker-name.
func GetAllLoadWorker(cli *clientv3.Client) (map[string]map[string]string, int64, error) {
	var (
		worker string
		tslwm  = make(map[string]map[string]string)
	)
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.LoadWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return tslwm, 0, err
	}

	for _, kv := range resp.Kvs {
		keys, err2 := common.LoadWorkerKeyAdapter.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}

		task := keys[0]
		source := keys[1]

		err = json.Unmarshal(kv.Value, &worker)
		if err != nil {
			return tslwm, 0, err
		}

		if _, ok := tslwm[task]; !ok {
			tslwm[task] = make(map[string]string)
		}
		tslwm[task][source] = worker
	}

	return tslwm, resp.Header.Revision, err
}

// WatchLoadWorker watches PUT & DELETE operations for worker in load stage.
// This function should often be called by DM-master.
func WatchLoadWorker(ctx context.Context, cli *clientv3.Client, revision int64,
	outCh chan<- LoadWorker, errCh chan<- error) {
	// NOTE: WithPrevKV used to get a valid `ev.PrevKv` for deletion.
	ch := cli.Watch(ctx, common.LoadWorkerKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.Canceled {
				select {
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					loadWorker LoadWorker
					err        error
					keys       []string
				)

				switch ev.Type {
				case mvccpb.PUT, mvccpb.DELETE:
					keys, err = common.LoadWorkerKeyAdapter.Decode(string(ev.Kv.Key))
					if err == nil {
						loadWorker.Task = keys[0]
						loadWorker.Source = keys[1]
						if ev.Type == mvccpb.PUT {
							err = json.Unmarshal(ev.Kv.Value, &loadWorker.Worker)
						} else {
							loadWorker.IsDelete = true
						}
					}
				default:
					// this should not happen.
					err = fmt.Errorf("unsupported ectd event type %v", ev.Type)
				}

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- loadWorker:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// PutLoadWorker puts the worker which load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker.
// This function should often be called by DM-worker.
func PutLoadWorker(cli *clientv3.Client, task, sourceID, worker string) (int64, error) {
	data, err := json.Marshal(worker)
	if err != nil {
		return 0, err
	}
	key := common.LoadWorkerKeyAdapter.Encode(task, sourceID)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, string(data))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// DelLoadWorker dels the worker in load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker.
func DelLoadWorker(cli *clientv3.Client, task, sourceID string) (int64, bool, error) {
	key := common.LoadWorkerKeyAdapter.Encode(task, sourceID)

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpDelete(key))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DelLoadWorkerByTask del the worker in load stage for the source by task.
func DelLoadWorkerByTask(cli *clientv3.Client, task string) (int64, bool, error) {
	key := common.LoadWorkerKeyAdapter.Encode(task)

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpDelete(key, clientv3.WithPrefix()))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}
