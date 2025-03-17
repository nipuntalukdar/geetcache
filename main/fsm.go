package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/hashicorp/raft"
	"io"

	"github.com/nipuntalukdar/geetcache/thrift"
)

type binLog struct {
	Command_code uint32
	Command_data interface{}
}

type applyRet struct {
	status thrift.Status
	resp   interface{}
}

func newApplyRet(st thrift.Status, resp interface{}) *applyRet {
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
		SLOG.Error("Deserialization", "error", err)
		return false
	}
	SLOG.Debug("Command to apply", "command", bin.Command_code)
	code := bin.Command_code
	switch code {
	case PUT:
		SLOG.Debug("PUT", "put", bin.Command_data)
		lput := bin.Command_data.(*thrift.PutCommand)
		status := cfsm.parts.put(lput.Key, lput.Data, lput.Expiry)
		return &applyRet{status: status, resp: nil}

	case LISTPUT:
		listput := bin.Command_data.(*thrift.ListPutCommand)
		status := cfsm.parts.list_put(listput.ListKey, listput.Values, listput.Append)
		return &applyRet{status: status, resp: nil}

	case LISTPOP:
		popc := bin.Command_data.(*thrift.ListGPCommand)
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

	case ADD_COUNTER:
		cadd := bin.Command_data.(*thrift.CAddCommand)
		status := cfsm.parts.add_counter(cadd.Name, cadd.InitialValue, cadd.Replace, 0)
		return newApplyRet(status, nil)

	case DEL_COUNTER:
		counter := bin.Command_data.(string)
		status := cfsm.parts.delete_counter(counter)
		return newApplyRet(status, nil)

	case CAS_COUNTER:
		counter := bin.Command_data.(*thrift.CASCommand)
		status := cfsm.parts.cmpswp_counter(counter.Name, counter.Expected, counter.UpdVal)
		return newApplyRet(status, nil)

	case INCR_COUNTER:
		incrcmd := bin.Command_data.(*thrift.CChangeCommand)
		status, val := cfsm.parts.increment_counter(incrcmd.Name, incrcmd.Delta, incrcmd.ReturnOld)
		return newApplyRet(status, val)

	case DECR_COUNTER:
		decrcmd := bin.Command_data.(*thrift.CChangeCommand)
		status, val := cfsm.parts.decrement_counter(decrcmd.Name, decrcmd.Delta, decrcmd.ReturnOld)
		return newApplyRet(status, val)

	case CREATE_HLL:
		hlcmd := bin.Command_data.(*thrift.HLogCreateCmd)
		status := cfsm.parts.hyperlog_create(hlcmd.Name, hlcmd.Expiry)
		return newApplyRet(status, nil)

	case DEL_HLL:
		hlkey := bin.Command_data.(string)
		status := cfsm.parts.hyperlog_delete(hlkey)
		return newApplyRet(status, nil)

	case ADD_HLL:
		haddcmd := bin.Command_data.(*thrift.HLogAddCmd)
		status := cfsm.parts.hyperlog_add(haddcmd.Key, murmur3_32(haddcmd.Data, 0))
		if status != thrift.Status_SUCCESS {
			SLOG.Error("Failed to add hll")
		}
		return newApplyRet(status, nil)

	default:
		return &applyRet{status: thrift.Status_BAD_COMMAND, resp: nil}
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
	if status != thrift.Status_SUCCESS {
		return errors.New("Failed to snapshot")
	}
	return nil
}
