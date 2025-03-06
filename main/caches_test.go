package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func TestCachesOps(t *testing.T) {
	t.Log("Testing caches")
	mycache := newPartitionCache(16)
	status := mycache.put("Hey", []byte("This is my value"), 0)
	if status != Status_SUCCESS {
		t.Fatalf("Put of key: Hey failed")
	}
	val, status := mycache.get("Hey")
	if status == SUCCESS {
		t.Logf("Hello: %s\n", val)
	} else {
		t.Fatal("put or get failed")
	}

	myarray := [][]byte{[]byte("value1"), []byte("value2")}
	status = mycache.list_put("list1", myarray, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	getval, status := mycache.list_get("list1", 100, true)
	if status != Status_SUCCESS || len(getval) != 2 {
		t.Fatal("Failed list_get")
	}
	getval, status = mycache.list_get("list1", 100, true)
	if status != Status_SUCCESS || len(getval) != 2 {
		t.Fatal("Failed list_get")
	}

	getval, status = mycache.list_pop("list1", 100, true)
	if status != Status_SUCCESS || len(getval) != 2 {
		t.Fatal("Failed list_get")
	}

	getval, status = mycache.list_pop("list1", 100, true)
	if status != Status_EMPTY_LIST || getval != nil {
		t.Fatal("Pop failed to clear elements")
	}
	getval, status = mycache.list_get("list1", 100, true)
	if status != Status_EMPTY_LIST || getval != nil {
		t.Fatal("Pop failed to clear elements")
	}
	status = mycache.list_put("list1", myarray, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	myarray2 := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	status = mycache.list_put("list2", myarray2, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	getval, status = mycache.list_get("list2", 1, true)
	if status != Status_SUCCESS || len(getval) != 1 {
		t.Fatalf("Failed list_geti to get exact number of items %d", len(getval))
	}
	status = mycache.delete_list("list2")
	if status != Status_SUCCESS {
		t.Fatal("Failed to delete list list2")
	}
	status = mycache.delete_list("list2")
	if status != Status_KEY_NOT_EXISTS {
		t.Fatal("Failed in invalid delete list")
	}
	for i := 0; i < 10000; i++ {
		status = mycache.put(fmt.Sprintf("Key%d", i), []byte("Value"), 0)
		if status != Status_SUCCESS {
			t.Fatalf("Put of key: %s failed", fmt.Sprintf("Key%d", i))
		}
	}
	for i := 0; i < 10000; i++ {
		status = mycache.delete(fmt.Sprintf("Key%d", i))
		if status != Status_SUCCESS {
			t.Fatalf("Delete of key: %s failed", fmt.Sprintf("Key%d", i))
		}
	}
	for i := 0; i < 10000; i++ {
		status = mycache.delete(fmt.Sprintf("Key%d", i))
		if status != Status_KEY_NOT_EXISTS {
			t.Fatalf("Delete of non-existing key: %s wrong result", fmt.Sprintf("Key%d", i))
		}
	}
	for i := 0; i < 10000; i++ {
		status = mycache.list_put(fmt.Sprintf("lst%d", i), myarray, true)
		if status != Status_SUCCESS {
			t.Fatalf("Put of list: %s failed", fmt.Sprintf("lst%d", i))
		}
	}
	for i := 0; i < 10000; i++ {
		status = mycache.delete_list(fmt.Sprintf("lst%d", i))
		if status != Status_SUCCESS {
			t.Fatalf("Delete of list: %s failed", fmt.Sprintf("lst%d", i))
		}
	}
	for i := 0; i < 10000; i++ {
		status = mycache.delete_list(fmt.Sprintf("lst%d", i))
		if status != Status_KEY_NOT_EXISTS {
			t.Fatalf("Delete of non-existing list: %s wrong result", fmt.Sprintf("lst%d", i))
		}
	}

	status = mycache.list_put("lencheck", myarray, true)
	if status != Status_SUCCESS {
		t.Fatalf("List put failed")
	}

	llen, status := mycache.list_len("lencheck")
	if status != Status_SUCCESS || int(llen) != len(myarray) {
		t.Fatalf("List len test failed")
	}

	status = mycache.delete_list("lencheck")
	if status != Status_SUCCESS {
		t.Fatalf("Delete of list: lencheck failed")
	}

	llen, status = mycache.list_len("lencheck")
	if status != Status_KEY_NOT_EXISTS || llen != 0 {
		t.Fatalf("List len test failed for no existing list")
	}
	status, _ = mycache.get_counter_value("counter1")
	if status != Status_KEY_NOT_EXISTS {
		t.Fatal("Counter test failed for non-existing counter")
	}
	status, _ = mycache.get_counter_value("counter1")
	if status != Status_KEY_NOT_EXISTS {
		t.Fatal("Counter test failed for non-existing counter")
	}
	status = mycache.add_counter("counter1", 100, false, 0)
	if status != Status_SUCCESS {
		t.Fatal("Counter test failed for adding counter")
	}

	status = mycache.add_counter("counter1", 100, false, 0)
	if status == Status_SUCCESS {
		t.Fatal("Counter test failed for adding counter")
	}
	status, cval := mycache.get_counter_value("counter1")
	if status != Status_SUCCESS || cval != 100 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
	status = mycache.add_counter("counter1", 200, true, 0)
	if status != Status_SUCCESS {
		t.Fatal("Failed to update counter")
	}
	status, cval = mycache.get_counter_value("counter1")
	if status != Status_SUCCESS || cval != 200 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
	status, cval = mycache.increment_counter("counter1", 100, false)
	if status != Status_SUCCESS || cval != 300 {
		t.Fatal("Counter test failed for incrementing counter value")
	} else {
		t.Logf("Counter new value is %d, expected!", cval)
	}
	status, cval = mycache.decrement_counter("counter1", 200, true)
	if status != Status_SUCCESS || cval != 300 {
		t.Fatal("Counter test failed for decrement counter value")
	} else {
		t.Logf("Counter old value is %d, expected!", cval)
	}
	status, cval = mycache.get_counter_value("counter1")
	if status != Status_SUCCESS || cval != 100 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
	status = mycache.cmpswp_counter("counter1", 100, 200)
	if status != Status_SUCCESS {
		t.Logf("Compare and swap test failed")
	}
	status, cval = mycache.get_counter_value("counter1")
	if status != Status_SUCCESS || cval != 200 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
	status = mycache.cmpswp_counter("counter1", 100, 200)
	if status != Status_FAILURE {
		t.Logf("Compare and swap test failed")
	}
	status = mycache.cmpswp_counter("counter2", 100, 200)
	if status != Status_KEY_NOT_EXISTS {
		t.Logf("Compare and swap test failed")
	}
	status = mycache.delete_counter("counter2")
	if status != Status_KEY_NOT_EXISTS {
		t.Logf("Delete counter test failed")
	}
	status = mycache.delete_counter("counter1")
	if status != Status_SUCCESS {
		t.Logf("Delete counter test failed")
	}
	status = mycache.delete_counter("counter1")
	if status != Status_KEY_NOT_EXISTS {
		t.Logf("Delete counter test failed")
	}
}

func TestCachePersist(t *testing.T) {
	mycache := newPartitionCache(16)
	status := mycache.put("Hey", []byte("This is my value"), 0)
	if status != SUCCESS {
		t.Error("put failed")
	}

	myarray := [][]byte{[]byte("value1"), []byte("value2")}
	status = mycache.list_put("list1", myarray, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	getval, status := mycache.list_get("list1", 100, true)
	if status != Status_SUCCESS || len(getval) != 2 {
		t.Fatal("Failed list_get")
	}

	status = mycache.list_put("list2", myarray, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	myarray2 := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	status = mycache.list_put("list3", myarray2, true)
	if status != Status_SUCCESS {
		t.Fatal("Failed list_put")
	}
	i := 0
	rand.Seed(int64(os.Getpid()))
	m := 10000 + rand.Intn(10000)
	for i < m {
		listname := fmt.Sprintf("biglist%d", i)
		j := 1 + rand.Intn(100)
		values := make([][]byte, j)
		k := 0
		for k < j {
			values[k] = []byte(fmt.Sprintf("%dvalues%d", k, j))
			k++
		}
		mycache.list_put(listname, values, i%2 == 0)
		i++
	}
	for i = 0; i < 200000; i++ {
		mycache.add_counter(fmt.Sprintf("counter%d", i), int64(i), true, 0)
	}
	defer os.Remove("a.bin")
	status = mycache.PersistInFile("a.bin")
	if status != Status_SUCCESS {
		t.Fatal("Persist test failed")
	}
	mycache = nil
	mycache2 := newPartitionCache(16)
	err := mycache2.RestoreFromFile("a.bin")
	if err != nil {
		t.Fatalf("Restore test failed %v\n", err)
	}
	status, cval := mycache2.get_counter_value("counter1978")
	if status != Status_SUCCESS || cval != 1978 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
	status, cval = mycache2.get_counter_value("counter1")
	if status != Status_SUCCESS || cval != 1 {
		t.Fatal("Counter test failed for getting counter value")
	} else {
		t.Logf("Counter value is %d, expected!", cval)
	}
}
