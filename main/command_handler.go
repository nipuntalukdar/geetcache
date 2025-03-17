package main

import (
	"context"
	"encoding/gob"
	"errors"

	"github.com/nipuntalukdar/geetcache/thrift"
)

type GeetCacheHandler struct {
	parts *partitionedCache
	rft   *raftInterface
	cfsm  *cacheFsm
	ctx   context.Context
}

func registerTypes() {
	gob.Register(thrift.NewListPutCommand())
	gob.Register(thrift.NewListGPCommand())
	gob.Register(thrift.NewPutCommand())
	gob.Register(thrift.NewCAddCommand())
	gob.Register(thrift.NewCASCommand())
	gob.Register(thrift.NewCChangeCommand())
	gob.Register(thrift.NewHLogCreateCmd())
	gob.Register(thrift.NewHLogAddCmd())
	gob.Register(thrift.NewHLogStatus())
}

func NewGeetCacheHandler() (*GeetCacheHandler, error) {
	registerTypes()
	numpartitions := CONFIG.NumPartitions
	parts := newPartitionCache(uint32(numpartitions))
	cacheFsm := newCacheFsm(parts)

	sstore := CONFIG.SStore
	logstore := CONFIG.Logs
	snapshotdir := CONFIG.Snapshot
	raftin, err := newRaftInterface(sstore, logstore, snapshotdir,
		cacheFsm)
	if err != nil {
		return nil, err
	}
	return &GeetCacheHandler{ctx: context.Background(), parts: parts, rft: raftin, cfsm: cacheFsm}, nil
}

func (gch *GeetCacheHandler) mutate(command_code uint32, data interface{}) (*applyRet, error) {
	bnl := newBinLog(command_code, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("Error Binlog", "error", err)
		return nil, err
	}
	applyret := gch.rft.Apply(sbnl, 0)
	if applyret.status != thrift.Status_SUCCESS {
		SLOG.Error("Failed apply status", "error", applyret.status)
		return nil, errors.New("apply failed")
	}
	return applyret, nil
}

func (gch *GeetCacheHandler) Leader(ctx context.Context) (*thrift.LeaderResponse, error) {
	leader := gch.rft.Leader()
	SLOG.Debug("Leader finding", "Leader", leader)
	lr := thrift.NewLeaderResponse()
	lr.Leader = leader
	return lr, nil
}

func (gch *GeetCacheHandler) Peers(context.Context) (*thrift.PeersResponse, error) {
	peers, err := gch.rft.Peers()
	pr := thrift.NewPeersResponse()
	if err == nil {
		pr.Stat = thrift.Status_SUCCESS
		pr.Peers = peers
	} else {
		pr.Stat = thrift.Status_FAILURE
	}
	return pr, nil
}

func (gch *GeetCacheHandler) Put(ctx context.Context, data *thrift.PutCommand) (thrift.Status, error) {
	//status := gch.parts.put(data.Key, data.Data, data.Expiry)
	bnl := newBinLog(PUT, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("PUT", "error", err)
		return thrift.Status_FAILURE, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	SLOG.Debug("PUT", "apply response", *applr)
	return applr.status, nil
}

func (gch *GeetCacheHandler) Get(ctx context.Context, key string) (*thrift.GetResponse, error) {
	SLOG.Debug("GET", "Key", key)
	data, status := gch.parts.get(key)
	if status != thrift.Status_SUCCESS {
		SLOG.Debug("GET failed", "Status", status)
		data = nil
	}
	return &thrift.GetResponse{Key: key, Status: status, Data: data}, nil
}

func (gch *GeetCacheHandler) ListPut(ctx context.Context, data *thrift.ListPutCommand) (thrift.Status, error) {
	bnl := newBinLog(LISTPUT, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		return thrift.Status_FAILURE, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	return applr.status, nil
}

func (gch *GeetCacheHandler) ListPop(ctx context.Context, command *thrift.ListGPCommand) (*thrift.ListGPResponse, error) {
	bnl := newBinLog(LISTPOP, command)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("LISTPOP", "error", err)
		return nil, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("LISTPOP", "error", err)
		return nil, err
	}
	lgpr := new(thrift.ListGPResponse)
	lgpr.ListKey = command.ListKey
	lgpr.Stat = thrift.Status_SUCCESS
	lgpr.Values = applr.resp.([][]byte)
	lgpr.Retlen = int32(len(lgpr.Values))

	return lgpr, nil
}

func (gch *GeetCacheHandler) ListGet(ctx context.Context, command *thrift.ListGPCommand) (*thrift.ListGPResponse, error) {
	lst, status := gch.parts.list_get(command.ListKey, command.MaxCount, command.Front)
	if status != thrift.Status_SUCCESS {
		return &thrift.ListGPResponse{ListKey: command.ListKey, Stat: status, Retlen: 0, Values: nil}, nil
	}
	return &thrift.ListGPResponse{ListKey: command.ListKey, Stat: status,
		Retlen: int32(len(lst)), Values: lst}, nil
}

func (gch *GeetCacheHandler) ListLen(ctx context.Context, key string) (*thrift.ListLenResponse, error) {
	len, status := gch.parts.list_len(key)
	return &thrift.ListLenResponse{ListKey: key, Stat: status, LLen: len}, nil
}

func (gch *GeetCacheHandler) Delete(ctx context.Context, key string) (*thrift.DelResponse, error) {
	applret, _ := gch.mutate(DELETE, key)
	dr := thrift.NewDelResponse()
	dr.Stat = applret.status
	dr.Key = key
	return dr, nil
}

func (gch *GeetCacheHandler) DeleteList(ctx context.Context, key string) (*thrift.DelResponse, error) {
	applret, _ := gch.mutate(DELETE_LIST, key)
	dr := thrift.NewDelResponse()
	dr.Stat = applret.status
	dr.Key = key
	return dr, nil
}

func (gch *GeetCacheHandler) AddCounter(ctx context.Context, counter *thrift.CAddCommand) (thrift.Status, error) {
	bnl := newBinLog(ADD_COUNTER, counter)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("ADD_COUNTER", counter.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("ADD_COUNTER APPLY STATUS", counter.Name, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) DeleteCounter(ctx context.Context, counterName string) (thrift.Status, error) {
	bnl := newBinLog(DEL_COUNTER, counterName)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("DELETE COUNTER", counterName, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("DEL_COUNTER", counterName, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) incrOrdecr(counter *thrift.CChangeCommand,
	command_code uint32) (*thrift.CStatus, error) {
	bnl := newBinLog(command_code, counter)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("incrOrdecr", counter.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("MODIFYING COUNTER", counter.Name, applr.status)
	}
	cs := thrift.NewCStatus()
	cs.Name = counter.Name
	cs.Stat = applr.status
	cs.Value = applr.resp.(int64)
	return cs, nil
}

func (gch *GeetCacheHandler) Increament(ctx context.Context, counter *thrift.CChangeCommand) (*thrift.CStatus, error) {
	return gch.incrOrdecr(counter, INCR_COUNTER)
}

func (gch *GeetCacheHandler) Decrement(ctx context.Context, counter *thrift.CChangeCommand) (*thrift.CStatus, error) {
	return gch.incrOrdecr(counter, DECR_COUNTER)
}

func (gch *GeetCacheHandler) CompSwap(ctx context.Context, cas *thrift.CASCommand) (thrift.Status, error) {
	bnl := newBinLog(CAS_COUNTER, cas)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("CAS_COUNTER", cas.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("CAS_COUNTER", cas.Name, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) GetCounterValue(ctx context.Context, counterName string) (*thrift.CStatus, error) {
	status, val := gch.parts.get_counter_value(counterName)
	cs := thrift.NewCStatus()
	cs.Stat = status
	cs.Name = counterName
	cs.Value = val
	return cs, nil
}

func (gch *GeetCacheHandler) HLogCreate(ctx context.Context, hcmd *thrift.HLogCreateCmd) (*thrift.HLogStatus, error) {
	bnl := newBinLog(CREATE_HLL, hcmd)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("CREATE_HLL", hcmd.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("CREATE_HLL", hcmd.Name, applr.status)
	}
	hls := thrift.NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = hcmd.Name
	return hls, nil
}

func (gch *GeetCacheHandler) HLogDelete(ctx context.Context, key string) (*thrift.HLogStatus, error) {
	bnl := newBinLog(DEL_HLL, key)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("DEL_HLL", key, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("DEL_HLL", key, applr.status)
	}
	hls := thrift.NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = key
	return hls, nil
}

func (gch *GeetCacheHandler) HLogCardinality(ctx context.Context, key string) (*thrift.HLogStatus, error) {
	val, status := gch.parts.hyperlog_cardinality(key)
	SLOG.Info("Cardinality", val, key)
	hls := thrift.NewHLogStatus()
	hls.Stat = status
	hls.Key = key
	if thrift.Status_SUCCESS == status {
		newval := new(int64)
		*newval = int64(val)
		hls.Value = newval
	}
	return hls, nil
}

func (gch *GeetCacheHandler) HLogAdd(ctx context.Context, hcmd *thrift.HLogAddCmd) (*thrift.HLogStatus, error) {
	bnl := newBinLog(ADD_HLL, hcmd)
	sbnl, err := bnl.serialize()
	if err != nil {
		SLOG.Error("ADD_HLL", hcmd.Key, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != thrift.Status_SUCCESS {
		SLOG.Error("ADD_HLL", hcmd.Key, applr.status)
	}
	hls := thrift.NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = hcmd.Key
	return hls, nil
}
