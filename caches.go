package main

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type onelist struct {
	lock   *sync.RWMutex
	values *list.List
}

type listcache struct {
	lock  *sync.RWMutex
	lists map[string]*onelist
}

type plaincache struct {
	lock   *sync.RWMutex
	pcache map[string][]byte
}

type caches struct {
	lcache *listcache
	pcache *plaincache
}

type partitionedCache struct {
	numpartitions uint32
	partitions    []*caches
	biglock       *sync.RWMutex
}

func newOneList() *onelist {
	lock := &sync.RWMutex{}
	lst := list.New()
	return &onelist{lock: lock, values: lst}
}

func newListCache() *listcache {
	lock := &sync.RWMutex{}
	lists := make(map[string]*onelist)
	return &listcache{lock: lock, lists: lists}
}

func newPlainCache() *plaincache {
	lock := &sync.RWMutex{}
	pcache := make(map[string][]byte)
	return &plaincache{lock: lock, pcache: pcache}
}

func newCaches() *caches {
	lcache := newListCache()
	pcache := newPlainCache()
	return &caches{lcache: lcache, pcache: pcache}
}

func newPartitionCache(numpartitions uint32) *partitionedCache {
	if numpartitions < 1 {
		numpartitions = MIN_PARTITIONS
	}
	if numpartitions > MAX_PARTITIONS {
		numpartitions = MAX_PARTITIONS
	}

	partitions := make([]*caches, numpartitions)
	var i uint32 = 0
	for ; i < numpartitions; i++ {
		partitions[i] = newCaches()
	}
	return &partitionedCache{numpartitions: numpartitions, partitions: partitions, biglock: &sync.RWMutex{}}
}

func (parts *partitionedCache) put(key string, data []byte, expiry int64) Status {
	parts.biglock.RLock()
	defer parts.biglock.RUnlock()
	partition := getPartition(key, parts.numpartitions)
	kvs := parts.partitions[partition].pcache
	kvs.lock.Lock()
	defer kvs.lock.Unlock()
	_, ok := kvs.pcache[key]
	if ok {
		return Status_KEY_EXISTS
	}
	copied := copyslice(data)
	kvs.pcache[key] = copied
	return Status_SUCCESS
}

func (parts *partitionedCache) delete(key string) Status {
	parts.biglock.RLock()
	defer parts.biglock.RUnlock()
	partition := getPartition(key, parts.numpartitions)
	kvs := parts.partitions[partition].pcache
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	_, ok := kvs.pcache[key]
	if !ok {
		return Status_KEY_NOT_EXISTS
	} else {
		delete(kvs.pcache, key)
		return Status_SUCCESS
	}
}

func (parts *partitionedCache) get(key string) ([]byte, Status) {
	partition := getPartition(key, parts.numpartitions)
	kvs := parts.partitions[partition].pcache
	kvs.lock.Lock()
	defer kvs.lock.Unlock()
	data, ok := kvs.pcache[key]
	if !ok {
		return nil, Status_KEY_NOT_EXISTS
	}
	copied := copyslice(data)
	return copied, Status_SUCCESS
}

func (parts *partitionedCache) list_put(key string, values [][]byte, append bool) Status {
	parts.biglock.RLock()
	defer parts.biglock.RUnlock()
	partition := getPartition(key, parts.numpartitions)
	listref := parts.partitions[partition].lcache
	listref.lock.Lock()
	data, ok := listref.lists[key]
	if !ok {
		data = newOneList()
		listref.lists[key] = data
	}
	listref.lock.Unlock()
	data.lock.Lock()
	defer data.lock.Unlock()
	for _, val := range values {
		if append {
			data.values.PushBack(val)
		} else {
			data.values.PushFront(val)
		}
	}
	return Status_SUCCESS
}

func (parts *partitionedCache) delete_list(key string) Status {
	parts.biglock.RLock()
	defer parts.biglock.RUnlock()
	partition := getPartition(key, parts.numpartitions)
	listref := parts.partitions[partition].lcache
	listref.lock.Lock()
	defer listref.lock.Unlock()

	_, ok := listref.lists[key]
	if !ok {
		LOG.Debugf("List not found: %s", key)
		return Status_KEY_NOT_EXISTS
	} else {
		delete(listref.lists, key)
		LOG.Debugf("Deleted list: %s", key)
		return Status_SUCCESS
	}
}

func (parts *partitionedCache) list_get(key string, numitem int32, front bool) ([][]byte, Status) {
	partition := getPartition(key, parts.numpartitions)
	listref := parts.partitions[partition].lcache
	listref.lock.Lock()
	defer listref.lock.Unlock()
	data, ok := listref.lists[key]
	if !ok {
		return nil, Status_KEY_NOT_EXISTS
	}
	data.lock.RLock()
	defer data.lock.RUnlock()
	if data.values.Len() == 0 {
		return nil, Status_EMPTY_LIST
	}

	if numitem < MIN_GET {
		numitem = MIN_GET
	}
	if numitem > MAX_GET {
		numitem = MAX_GET
	}

	if numitem > int32(data.values.Len()) {
		numitem = int32(data.values.Len())
	}

	var i int32 = 0
	out := make([][]byte, numitem)
	if front {
		for e := data.values.Front(); e != nil; e = e.Next() {
			out[i] = e.Value.([]byte)
			i++
			if i == numitem {
				break
			}
		}
	} else {
		numitem--
		for e := data.values.Back(); e != nil; e = e.Prev() {
			out[numitem-i] = e.Value.([]byte)
			if i == numitem {
				break
			}
			i++
		}
	}
	return out, Status_SUCCESS
}

func (parts *partitionedCache) list_pop(key string, numitem int32, front bool) ([][]byte,
	Status) {
	parts.biglock.RLock()
	defer parts.biglock.RUnlock()
	partition := getPartition(key, parts.numpartitions)
	listref := parts.partitions[partition].lcache
	listref.lock.Lock()
	defer listref.lock.Unlock()
	data, ok := listref.lists[key]
	if !ok {
		return nil, Status_KEY_NOT_EXISTS
	}
	data.lock.Lock()
	defer data.lock.Unlock()
	if data.values.Len() == 0 {
		return nil, Status_EMPTY_LIST
	}

	if numitem < 0 {
		numitem = MIN_GET
	}
	if numitem > MAX_GET {
		numitem = MAX_GET
	}
	if numitem > int32(data.values.Len()) {
		numitem = int32(data.values.Len())
	}
	var temp *list.Element = nil
	var i int32 = 0
	out := make([][]byte, numitem)
	numitem--
	for ; i <= numitem; i++ {
		if front {
			temp = data.values.Front()
		} else {
			temp = data.values.Back()
		}
		if temp == nil {
			break
		}
		if front {
			out[i] = temp.Value.([]byte)
		} else {
			out[numitem-i] = temp.Value.([]byte)
		}
		data.values.Remove(temp)
	}
	return out, Status_SUCCESS
}

func (parts *partitionedCache) dumpPartition(partno uint32, c *caches, writer io.Writer) error {
	for key, lst := range c.lcache.lists {
		if lst.values.Len() == 0 {
			continue
		}
		errp(writeUint32(partno, writer))
		errp(writeUint8(LISTTYPE, writer))
		errp(writeString(key, writer))
		errp(writeUint32(uint32(lst.values.Len()), writer))
		for e := lst.values.Front(); e != nil; e = e.Next() {
			errp(writeByteArray(e.Value.([]byte), writer))
		}
	}
	pcachelen := len(c.pcache.pcache)
	if pcachelen == 0 {
		return nil
	}
	errp(writeUint32(partno, writer))
	errp(writeUint8(PMAPTYPE, writer))
	errp(writeUint32(uint32(pcachelen), writer))
	for k, v := range c.pcache.pcache {
		errp(writeString(k, writer))
		errp(writeByteArray(v, writer))
	}
	return nil
}

func (parts *partitionedCache) PersistInFile(filePath string) Status {
	file, err := os.Create(filePath)
	if err != nil {
		return Status_FAILURE
	}
	return parts.Persist(file)
}

func (parts *partitionedCache) Persist(file io.WriteCloser) Status {
	parts.biglock.Lock()
	defer parts.biglock.Unlock()
	defer file.Close()
	writer := bufio.NewWriter(file)
	errp(writeUint32(MAGIC, writer))
	errp(writeUint32(parts.numpartitions, writer))
	// Write only the non-empty partitions
	for i, c := range parts.partitions {
		parts.dumpPartition(uint32(i), c, writer)
	}
	writer.Flush()
	file.Close()
	return Status_SUCCESS
}

func (parts *partitionedCache) getPMap(reader io.Reader) (map[string][]byte, error) {
	// first get the number of elements for the map
	numelem, err := readUint32(reader)
	if err != nil {
		return nil, err
	}

	mp := make(map[string][]byte)
	// Now read the key value pairs
	for numelem > 0 {
		key, err := readString(reader)
		if err != nil {
			return nil, err
		}
		b, err := readByteArray(reader)
		if err != nil {
			return nil, err
		}
		mp[key] = b
		numelem--
	}
	return mp, nil
}

func (parts *partitionedCache) getList(reader io.Reader) (*list.List, string, error) {
	// first get the number of elements from the list
	key, err := readString(reader)
	if err != nil {
		return nil, "", err
	}
	numelem, err := readUint32(reader)
	if err != nil {
		return nil, "", err
	}
	lst := list.New()
	for numelem > 0 {
		b, err := readByteArray(reader)
		if err != nil {
			return nil, "", err
		}
		if key == "biglist18" {
		}
		lst.PushBack(b)
		numelem--
	}
	return lst, key, nil
}

func (parts *partitionedCache) RestoreFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	return parts.Restore(file)
}

func (parts *partitionedCache) Restore(file io.ReadCloser) error {
	parts.biglock.Lock()
	defer parts.biglock.Unlock()
	defer file.Close()
	reader := bufio.NewReader(file)
	magic, err := readUint32(reader)
	if err != nil {
		return err
	}
	if MAGIC != magic {
		return errors.New("Corrupt snapshot")
	}
	numparts, err := readUint32(reader)
	parts.numpartitions = numparts
	if err != nil {
		return err
	}
	pmap_found := make(map[uint32]bool)
	for {
		partno, err := readUint32(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		typ, err := readUint8(reader)
		if err != nil {
			return err
		}
		if !validType(typ) {
			return errors.New("Invalid type detected")
		}
		if typ == LISTTYPE {
			lst, key, err := parts.getList(reader)
			if err != nil {
				return err
			}
			listref := parts.partitions[partno].lcache
			data, ok := listref.lists[key]
			if !ok {
				data = newOneList()
				listref.lists[key] = data
			} else {
				listref.lists[key].values = lst
			}
		} else if typ == PMAPTYPE {
			_, ok := pmap_found[partno]
			if ok {
				return errors.New(fmt.Sprintf("Multiple pmap found for partition %d", partno))
			}
			pmap_found[partno] = true
			pmap, err := parts.getPMap(reader)
			if err != nil {
				return err
			}
			parts.partitions[partno].pcache.pcache = pmap
		}
	}
	return nil
}
