package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
)

type binLog struct {
	Command_code uint32
	Command_data interface{}
}

type applyRet struct {
	status Status
	resp   interface{}
}

func newApplyRet(st Status, resp interface{}) *applyRet {
	return &applyRet{status: st, resp: resp}
}

func newBinLog(command_code uint32, command_data interface{}) *binLog {
	return &binLog{Command_code: command_code, Command_data: command_data}
}

func (bin *binLog) serialize() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(bin)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func deserBinLog(buffer []byte) (*binLog, error) {
	decoder := gob.NewDecoder(bytes.NewBuffer(buffer))
	var ret *binLog
	err := decoder.Decode(&ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

type cacheFsm struct {
	parts *partitionedCache
}

type cacheFsmSnapshot struct {
	fsm *cacheFsm
}

func newCacheFsmSnapshot(fsm *cacheFsm) *cacheFsmSnapshot {
	return &cacheFsmSnapshot{fsm: fsm}
}

func (cfsm *cacheFsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return cfsm.fsm.Persist(sink)
}

func (cfsm *cacheFsmSnapshot) Release() {
}

func newCacheFsm(parts *partitionedCache) *cacheFsm {
	return &cacheFsm{parts: parts}
}

func (cfsm *cacheFsm) Apply(log *raft.Log) interface{} {
	bin, err := deserBinLog(log.Data)
	if err != nil {
		return false
	}
	code := bin.Command_code
	switch code {
	case PUT:
		fmt.Printf("Putting %v\n", bin.Command_data)
		lput := bin.Command_data.(*PutCommand)
		status := cfsm.parts.put(lput.Key, lput.Data, lput.Expiry)
		return &applyRet{status: status, resp: nil}

	case LISTPUT:
		listput := bin.Command_data.(*ListPutCommand)
		status := cfsm.parts.list_put(listput.ListKey, listput.Values, listput.Append)
		return &applyRet{status: status, resp: nil}

	case LISTPOP:
		popc := bin.Command_data.(*ListGPCommand)
		pops, status := cfsm.parts.list_pop(popc.ListKey, popc.MaxCount, popc.Front)
		return &applyRet{status: status, resp: pops}

	case DELETE:
		key := bin.Command_data.(string)
		status := cfsm.parts.delete(key)
		return &applyRet{status: status, resp: nil}

	case DELETE_LIST:
		key := bin.Command_data.(string)
		status := cfsm.parts.delete_list(key)
		return &applyRet{status: status, resp: nil}

	default:
		return &applyRet{status: Status_BAD_COMMAND, resp: nil}
	}
}

func (cfsm *cacheFsm) Snapshot() (raft.FSMSnapshot, error) {
	return newCacheFsmSnapshot(cfsm), nil
}

func (cfsm *cacheFsm) Restore(reader io.ReadCloser) error {
	return cfsm.parts.Restore(reader)
}

func (cfsm *cacheFsm) Persist(sink raft.SnapshotSink) error {
	status := cfsm.parts.Persist(sink)
	if status != Status_SUCCESS {
		return errors.New("Failed to snapshot")
	}
	return nil
}
