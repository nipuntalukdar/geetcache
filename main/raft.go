package main

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nipuntalukdar/geetcache/thrift"
	"time"
)

type raftInterface struct {
	stableStorePath string
	logStorePath    string
	snapshotPath    string
	transport       string
	rft             *raft.Raft
}

func newRaftInterface(spath string, lpath string, snpath string,
	fsm raft.FSM) (*raftInterface, error) {
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

	my_raft_addr := CONFIG.GetRaftAddress(CONFIG.MyID)
	if my_raft_addr == "" {
		SLOG.Fatal("Invadli Raft address")
	}
	transport, err := raft.NewTCPTransport(my_raft_addr, nil, 10, 10*time.Second, SLOG_WRITER)
	if err != nil {
		return nil, err
	}

	conf := raft.DefaultConfig()
	conf.SnapshotThreshold = 400
	conf.SnapshotInterval = 120 * time.Second
	conf.Logger = SLOG

	rft, err := raft.NewRaft(conf, fsm, logstore, sstore, snaps, transport)
	if err != nil {
		return nil, err
	}
	return &raftInterface{stableStorePath: spath, logStorePath: lpath,
		snapshotPath: snpath, transport: my_raft_addr, rft: rft}, nil
}

func (rftin *raftInterface) Apply(mutating_command []byte, t time.Duration) *applyRet {
	future := rftin.rft.Apply(mutating_command, t)
	if err := future.Error(); err != nil {
		SLOG.Error("ratft apply", "error", err)
		return newApplyRet(thrift.Status_FAILURE, nil)
	}
	return future.Response().(*applyRet)
}

func (rftin *raftInterface) Peers() ([]string, error) {
	return nil, nil
}

func (rftin *raftInterface) Leader() string {
	return string(rftin.rft.Leader())
}
