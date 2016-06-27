package main

import (
	"errors"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"time"
)

type raftInterface struct {
	stableStorePath string
	logStorePath    string
	snapshotPath    string
	peerPath        string
	transport       string
	rft             *raft.Raft
	peerStore       raft.PeerStore
}

func newRaftInterface(spath string, lpath string, snpath string,
	peerpath string, fsm raft.FSM) (*raftInterface, error) {
	sstore, err := raftboltdb.NewBoltStore(spath)
	if err != nil {
		return nil, err
	}

	logstore, err := raftboltdb.NewBoltStore(lpath)
	if err != nil {
		return nil, err
	}

	snaps, err := raft.NewFileSnapshotStoreWithLogger(snpath, 3, SLOG)
	if err != nil {
		return nil, err
	}

	my_raft_addr, err := getConfig().getMyAddr()
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(my_raft_addr, nil, 10, 10*time.Second, SLOG_WRITER)
	if err != nil {
		return nil, err
	}

	peerstore := raft.NewJSONPeers(peerpath, transport)
	conf := raft.DefaultConfig()
	conf.EnableSingleNode = true
	conf.SnapshotThreshold = 400
	conf.SnapshotInterval = 120 * time.Second
	conf.Logger = SLOG

	rft, err := raft.NewRaft(conf, fsm, logstore, sstore, snaps, peerstore, transport)
	if err != nil {
		return nil, err
	}
	return &raftInterface{stableStorePath: spath, logStorePath: lpath, snapshotPath: snpath,
		peerPath: peerpath, rft: rft, peerStore: peerstore}, nil
}

func (rftin *raftInterface) Apply(mutating_command []byte, t time.Duration) *applyRet {
	future := rftin.rft.Apply(mutating_command, t)
	if err := future.Error(); err != nil {
		return newApplyRet(Status_FAILURE, nil)
	}
	return future.Response().(*applyRet)
}

func (rftin *raftInterface) Peers() ([]string, error) {
	peers, err := rftin.peerStore.Peers()
	if err != nil {
		LOG.Errorf("Failure in getting peers %s", err)
		return nil, errors.New("Peers error")
	}
	return peers, nil
}

func (rftin *raftInterface) Leader() string {
	return rftin.rft.Leader()
}
