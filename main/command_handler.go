package main

import (
	"encoding/gob"
	"errors"
)

type GeetCacheHandler struct {
	parts *partitionedCache
	rft   *raftInterface
	cfsm  *cacheFsm
}

func registerTypes() {
	gob.Register(NewListPutCommand())
	gob.Register(NewListGPCommand())
	gob.Register(NewPutCommand())
	gob.Register(NewCAddCommand())
	gob.Register(NewCASCommand())
	gob.Register(NewCChangeCommand())
	gob.Register(NewHLogCreateCmd())
	gob.Register(NewHLogAddCmd())
	gob.Register(NewHLogStatus())
}

func NewGeetCacheHandler() (*GeetCacheHandler, error) {
	registerTypes()
	numpartitions, err := getConfig().getNumPartition()
	if err != nil {
		LOG.Fatalf("Unable to get number of partitions: %v", err)
	}
	parts := newPartitionCache(uint32(numpartitions))
	cacheFsm := newCacheFsm(parts)

	sstore, err := getConfig().getStableStoreDir()
	if err != nil {
		LOG.Fatalf("Unable to get raft stable store: %v", err)
	}
	logstore, err := getConfig().getLogDir()
	if err != nil {
		LOG.Fatalf("Unable to get raft log store: %v", err)
	}
	snapshotdir, err := getConfig().getSnapshotDir()
	if err != nil {
		LOG.Fatalf("Unable to get raft snapshot dir: %v", err)
	}
	peerjson, err := getConfig().getPeerListDir()
	if err != nil {
		LOG.Fatalf("Unable to get raft peerlist dir: %v", err)
	}
	raftin, err := newRaftInterface(sstore, logstore, snapshotdir,
		peerjson, cacheFsm)
	if err != nil {
		return nil, err
	}
	return &GeetCacheHandler{parts: parts, rft: raftin, cfsm: cacheFsm}, nil
}

func (gch *GeetCacheHandler) mutate(command_code uint32, data interface{}) (*applyRet, error) {
	bnl := newBinLog(command_code, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("Error %s", err)
		return nil, err
	}
	applyret := gch.rft.Apply(sbnl, 0)
	if applyret.status != Status_SUCCESS {
		LOG.Errorf("Failed apply status %s", applyret.status)
		return nil, errors.New("apply failed")
	}
	return applyret, nil
}

func (gch *GeetCacheHandler) Leader() (*LeaderResponse, error) {
	leader := gch.rft.Leader()
	LOG.Debugf("Leader is %s", leader)
	lr := NewLeaderResponse()
	lr.Leader = leader
	return lr, nil
}

func (gch *GeetCacheHandler) Peers() (*PeersResponse, error) {
	peers, err := gch.rft.Peers()
	pr := NewPeersResponse()
	if err == nil {
		LOG.Debugf("Peers: %s", peers)
		pr.Stat = Status_SUCCESS
		pr.Peers = peers
	} else {
		pr.Stat = Status_FAILURE
	}
	return pr, nil
}

func (gch *GeetCacheHandler) Put(data *PutCommand) (Status, error) {
	//status := gch.parts.put(data.Key, data.Data, data.Expiry)
	bnl := newBinLog(PUT, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("Error %s", err)
		return Status_FAILURE, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	LOG.Debugf("Put apply response %s", *applr)
	return applr.status, nil
}

func (gch *GeetCacheHandler) Get(key string) (*GetResponse, error) {
	LOG.Debugf("Get for key %s", key)
	data, status := gch.parts.get(key)
	if status != Status_SUCCESS {
		LOG.Debugf("Failed get status %s", status)
		data = nil
	}
	return &GetResponse{Key: key, Status: status, Data: data}, nil
}

func (gch *GeetCacheHandler) ListPut(data *ListPutCommand) (Status, error) {
	bnl := newBinLog(LISTPUT, data)
	sbnl, err := bnl.serialize()
	if err != nil {
		return Status_FAILURE, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	return applr.status, nil
}

func (gch *GeetCacheHandler) ListPop(command *ListGPCommand) (*ListGPResponse, error) {
	bnl := newBinLog(LISTPOP, command)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("List pop %s", err)
		return nil, err
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("List pop %s", err)
		return nil, err
	}
	lgpr := new(ListGPResponse)
	lgpr.ListKey = command.ListKey
	lgpr.Stat = Status_SUCCESS
	lgpr.Values = applr.resp.([][]byte)
	lgpr.Retlen = int32(len(lgpr.Values))

	return lgpr, nil
}

func (gch *GeetCacheHandler) ListGet(command *ListGPCommand) (*ListGPResponse, error) {
	lst, status := gch.parts.list_get(command.ListKey, command.MaxCount, command.Front)
	if status != Status_SUCCESS {
		return &ListGPResponse{ListKey: command.ListKey, Stat: status, Retlen: 0, Values: nil}, nil
	}
	return &ListGPResponse{ListKey: command.ListKey, Stat: status,
		Retlen: int32(len(lst)), Values: lst}, nil
}

func (gch *GeetCacheHandler) ListLen(key string) (*ListLenResponse, error) {
	len, status := gch.parts.list_len(key)
	return &ListLenResponse{ListKey: key, Stat: status, LLen: len}, nil
}

func (gch *GeetCacheHandler) Delete(key string) (*DelResponse, error) {
	applret, _ := gch.mutate(DELETE, key)
	dr := NewDelResponse()
	dr.Stat = applret.status
	dr.Key = key
	return dr, nil
}

func (gch *GeetCacheHandler) DeleteList(key string) (*DelResponse, error) {
	applret, _ := gch.mutate(DELETE_LIST, key)
	dr := NewDelResponse()
	dr.Stat = applret.status
	dr.Key = key
	return dr, nil
}

func (gch *GeetCacheHandler) AddCounter(counter *CAddCommand) (Status, error) {
	bnl := newBinLog(ADD_COUNTER, counter)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("add counter %s: %s", counter.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("Add counter:%s, %s", counter.Name, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) DeleteCounter(counterName string) (Status, error) {
	bnl := newBinLog(DEL_COUNTER, counterName)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("Delete counter %s: %s", counterName, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("Delete counter:%s, %s ", counterName, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) incrOrdecr(counter *CChangeCommand,
	command_code uint32) (*CStatus, error) {
	bnl := newBinLog(command_code, counter)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("changing counter %s: %s", counter.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("changing counter:%s, %s", counter.Name, applr.status)
	}
	cs := NewCStatus()
	cs.Name = counter.Name
	cs.Stat = applr.status
	cs.Value = applr.resp.(int64)
	return cs, nil
}

func (gch *GeetCacheHandler) Increament(counter *CChangeCommand) (*CStatus, error) {
	return gch.incrOrdecr(counter, INCR_COUNTER)
}

func (gch *GeetCacheHandler) Decrement(counter *CChangeCommand) (*CStatus, error) {
	return gch.incrOrdecr(counter, DECR_COUNTER)
}

func (gch *GeetCacheHandler) CompSwap(cas *CASCommand) (Status, error) {
	bnl := newBinLog(CAS_COUNTER, cas)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("CAS counter %s: %s", cas.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("CAS counter:%s, %s ", cas.Name, applr.status)
	}
	return applr.status, nil
}

func (gch *GeetCacheHandler) GetCounterValue(counterName string) (*CStatus, error) {
	status, val := gch.parts.get_counter_value(counterName)
	cs := NewCStatus()
	cs.Stat = status
	cs.Name = counterName
	cs.Value = val
	return cs, nil
}

func (gch *GeetCacheHandler) HLogCreate(hcmd *HLogCreateCmd) (*HLogStatus, error) {
	bnl := newBinLog(CREATE_HLL, hcmd)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("HLL create %s: %s", hcmd.Name, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("HLL create:%s, %s ", hcmd.Name, applr.status)
	}
	hls := NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = hcmd.Name
	return hls, nil
}

func (gch *GeetCacheHandler) HLogDelete(key string) (*HLogStatus, error) {
	bnl := newBinLog(DEL_HLL, key)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("Delete hyperlog %s: %s", key, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("Delete hyperlog:%s, %s ", key, applr.status)
	}
	hls := NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = key
	return hls, nil
}

func (gch *GeetCacheHandler) HLogCardinality(key string) (*HLogStatus, error) {
	val, status := gch.parts.hyperlog_cardinality(key)
	LOG.Infof("Cardinality %d, key:%s", val, key)
	hls := NewHLogStatus()
	hls.Stat = status
	hls.Key = key
	if Status_SUCCESS == status {
		newval := new(int64)
		*newval = int64(val)
		hls.Value = newval
	}
	return hls, nil
}

func (gch *GeetCacheHandler) HLogAdd(hcmd *HLogAddCmd) (*HLogStatus, error) {
	bnl := newBinLog(ADD_HLL, hcmd)
	sbnl, err := bnl.serialize()
	if err != nil {
		LOG.Errorf("HLL Add1 %s: %s", hcmd.Key, err)
	}
	applr := gch.rft.Apply(sbnl, 0)
	if applr.status != Status_SUCCESS {
		LOG.Errorf("HLL Add2 :%s, %s ", hcmd.Key, applr.status)
	}
	hls := NewHLogStatus()
	hls.Stat = applr.status
	hls.Key = hcmd.Key
	return hls, nil
}
