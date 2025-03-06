package main

import (
	"testing"
)

func TestConfig(t *testing.T) {
	conf, err := initConfigS([]byte(default_config))
	if err != nil {
		t.Fatal("Failed tp parse config")
	}

	id, _ := conf.getMyId()
	if id != 1 {
		t.Fatal("Could not fetch id correctly")
	}
	t.Log("TestConfig success")
}
