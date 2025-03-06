package main

import (
	"testing"
)

func TestLogging(t *testing.T) {
	initConfigS([]byte(default_config))
	for i := 0; i < 1000000; i++ {
		LOG.Infof("Hello log %d", i)
	}
}
