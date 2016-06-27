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
	gob.Register(new(ListPutCommand))
	gob.Register(new(ListGPCommand))
	gob.Register(new(PutCommand))
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
	LOG.Debugf("Put apply response %s\n", *applr)
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
