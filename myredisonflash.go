package main

import "fmt"
import "encoding/json"
import "os"
import "time"
import "sync"
import "hash/crc32"

const (
  // IEEE is by far and away the most common CRC-32 polynomial.
  // Used by ethernet (IEEE 802.3), v.42, fddi, gzip, zip, png, ...
  IEEE = 0xedb88320
)

const (
	CREATE_MUTEXES = 4096
	DAYS = 7
	HOURS = 24
	SECONDS_IN_HOUR=3600
)

type TimeSeriesRecord struct {
	dirty bool `json:"-"`
	rwLock *sync.RWMutex `json:"-"`
	StartTimestampHour uint64
	Series [HOURS*DAYS]uint64 /* can count something upto 4G/4Billion per hour (after that it will overflow) , 24 hours a day(upto 7 days in memory)*/
	Next *TimeSeriesRecord
}

type Store struct {
	BigLock sync.RWMutex
	crc32_table *crc32.Table
	db map[string]*TimeSeriesRecord
	filename string
	directoryname string
	doneStoreExpiredEvictor chan bool
	doneStoreDiskFlusher chan bool
	queueStoreDiskFlusher chan string
	createMutexes[CREATE_MUTEXES] sync.Mutex
}

func StoreDiskFlusher(k *Store) {
	for {
		select {
		case <-k.doneStoreDiskFlusher:
			fmt.Println("StoreDiskFlusher stopped.");
			return
		case keyToFlush := <- k.queueStoreDiskFlusher:
			k.BigLock.RLock()
			var timeSeriesRecordPtr *TimeSeriesRecord
			timeSeriesRecordPtr = k.db[keyToFlush]
			if nil == timeSeriesRecordPtr {
				//Fatal
			} else {
				timeSeriesRecordPtr.rwLock.Lock()
				if timeSeriesRecordPtr.Next == nil {
					//Fatal
				} else {
					rwLockTmp := timeSeriesRecordPtr.rwLock
					if true == timeSeriesRecordPtr.dirty {
						var TimeSeriesRecordCopy TimeSeriesRecord
						TimeSeriesRecordCopy = *timeSeriesRecordPtr
						TimeSeriesRecordCopy.Next = nil
						tJSON, _ := json.Marshal(TimeSeriesRecordCopy)
						f, _ := os.OpenFile(k.directoryname + keyToFlush, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						tJSON = append(tJSON, "\n"...)
						f.Write(tJSON) 
						f.Close() //write to disk
					}
					*timeSeriesRecordPtr = *(timeSeriesRecordPtr.Next) // delete node from linked list(copy mechanism)
					timeSeriesRecordPtr.rwLock = rwLockTmp
				}
				timeSeriesRecordPtr.rwLock.Unlock()
			}
			k.BigLock.RUnlock()
		}
	}
}

func (k *Store) StartStoreDiskFlusher() {
	k.doneStoreDiskFlusher = make(chan bool)
	k.queueStoreDiskFlusher = make(chan string, 1000*1000)
	go StoreDiskFlusher(k)
	fmt.Println("StoreDiskFlusher started...");
}

func (k Store) StopStoreDiskFlusher() {
	k.doneStoreDiskFlusher <- true
}

func StoreExpiredEvictor(k *Store) {
	ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-k.doneStoreExpiredEvictor:
			fmt.Println("StoreExpiredEvictor stopped.");
			return
		case t := <-ticker.C:
			fmt.Println("Eviction triggered at", t)
		}
	}
}

func NewStore(filename string, directoryname string) *Store {
	store := &Store {
		db: map[string]*TimeSeriesRecord{},
		filename: filename,
		directoryname: directoryname,
	}
	store.crc32_table = crc32.MakeTable(IEEE)
	return store
}

func (k *Store) StartStoreExpiredEvictor() {
	k.doneStoreExpiredEvictor = make(chan bool)
	go StoreExpiredEvictor(k)
	fmt.Println("StoreExpiredEvictor started...");
}

func (k Store) StopStoreExpiredEvictor() {
	k.doneStoreExpiredEvictor <- true
}

func (k Store) Get(key string) *TimeSeriesRecord {
	k.BigLock.RLock()
	var timeSeriesRecordPtr *TimeSeriesRecord
	timeSeriesRecordPtr = k.db[key]
	if nil == timeSeriesRecordPtr {
		k.BigLock.RUnlock()
		return nil
	}
	var retTimeSeriesRecord TimeSeriesRecord
	timeSeriesRecordPtr.rwLock.RLock()
	retTimeSeriesRecord = *timeSeriesRecordPtr
	timeSeriesRecordPtr.rwLock.RUnlock()
	k.BigLock.RUnlock()
	return &retTimeSeriesRecord
}

func GetEpochTimestampHour(timestamp uint64) uint64 {
	if 0 == timestamp {
		timestamp = uint64(time.Now().Unix()) // epoch
	}
	return uint64(timestamp/SECONDS_IN_HOUR)*SECONDS_IN_HOUR  // closest past hour near timestamp
}


func (k Store) Put(key string, timestamp uint64, value uint64) bool {
	k.BigLock.RLock()
	var timeSeriesRecordPtr *TimeSeriesRecord
	timeSeriesRecordPtr = k.db[key]
	if nil == timeSeriesRecordPtr {
		k.BigLock.RUnlock()
		return false // please do Create() before doing Put()
	}
	var timestampHour = GetEpochTimestampHour(timestamp)
	timeSeriesRecordPtr.rwLock.Lock()
	if timestampHour < timeSeriesRecordPtr.StartTimestampHour {
		//fatal - only RAM-memory time frame data can be updated(not beyond that) - within window only not past than that
		timeSeriesRecordPtr.rwLock.Unlock()
		k.BigLock.RUnlock()
		return false
	}
	var curr *TimeSeriesRecord
	var prev *TimeSeriesRecord
	curr = timeSeriesRecordPtr
	for {
		if curr == nil {
			break
		}
		endTimeStampHour := (curr.StartTimestampHour + DAYS * HOURS * SECONDS_IN_HOUR) - 1
		if timestampHour < endTimeStampHour {
			break
		}
		prev = curr
		curr = curr.Next
	}
	if curr == nil {
		if prev.dirty == true {
			var newTimeSeriesRecord TimeSeriesRecord
			newTimeSeriesRecord.StartTimestampHour = GetEpochTimestampHour(timestampHour)
			curr = &newTimeSeriesRecord
			prev.Next = curr
			k.queueStoreDiskFlusher <- key
		} else {
			curr = prev
			curr.StartTimestampHour = GetEpochTimestampHour(timestampHour)
		}
	}
	seriesIdx := (timestampHour/SECONDS_IN_HOUR) - (curr.StartTimestampHour/SECONDS_IN_HOUR)
	if seriesIdx >= HOURS * DAYS {
		//fatal - within window only not beyond that
		timeSeriesRecordPtr.rwLock.Unlock()
		k.BigLock.RUnlock()
		return false
	}
	curr.Series[seriesIdx] += value
	curr.dirty = true
	timeSeriesRecordPtr.rwLock.Unlock()
	k.BigLock.RUnlock()
	return true
}

func (k Store) Create(key string, timestamp uint64) {
	k.BigLock.RLock()
	var newTimeSeriesRecord TimeSeriesRecord
	newTimeSeriesRecord.StartTimestampHour = GetEpochTimestampHour(timestamp)
	var rwLock sync.RWMutex
	newTimeSeriesRecord.rwLock = &rwLock
	var key_crc32 uint32
	key_crc32 = crc32.Checksum([]byte(key), k.crc32_table)
	k.createMutexes[key_crc32 % CREATE_MUTEXES].Lock()
	if nil == k.db[key] { /* create timeseries (if not present) */
		k.db[key] = &newTimeSeriesRecord
	}
	k.createMutexes[key_crc32 % CREATE_MUTEXES].Unlock()
	k.BigLock.RUnlock()
}

func (k Store) Delete(key string) {
	k.BigLock.RLock()
	delete(k.db, key);
	os.Remove(k.directoryname + key)
	k.BigLock.RUnlock()
}

func (k Store) Backup() {
	k.BigLock.Lock()
	mapJSON, _ := json.Marshal(k.db)
	file, _ := os.Create(k.filename)
	file.Write(mapJSON)
	file.Close()
	k.BigLock.Unlock()
}

func (k Store) Restore() { /* should be fired only during startup (before anything else) */
	k.BigLock.Lock()
	file, _ := os.Open(k.filename)
	fileinfo, _ := file.Stat()
	mapJSON := make([]byte, fileinfo.Size())
	file.Read(mapJSON)
	json.Unmarshal(mapJSON, &k.db)
	file.Close()
	k.BigLock.Unlock()
}

func test(KVStore *Store) {
		fmt.Println(KVStore.Get("ritesh2"))
	KVStore.Create("ritesh2",3600)
	//fmt.Println(KVStore.Get("ritesh2"))
	//KVStore.Create("ritesh2",3600)
	//fmt.Println(KVStore.Get("ritesh2"))
	//KVStore.Put("ritesh2",3600,101)
	//KVStore.Put("ritesh2",6084000,101)
	fmt.Println(KVStore.Get("ritesh2"))

	var i uint64
	for i = 0; i < 24*7; i++ {
		KVStore.Put("ritesh2",3600+i*3600,100+i)
		//KVStore.Put("ritesh2",6084000+i*3600,100+i)
		//KVStore.Put("ritesh2",GetEpochTimestampHour(0)+i*3600,100+i)
	}
	fmt.Println("checkpoint")
	for i = 0; i < 24*7; i++ {
		//KVStore.Put("ritesh2",3600+i*3600,100+i)
		KVStore.Put("ritesh2",6084000+i*3600,100+i)
		//KVStore.Put("ritesh2",GetEpochTimestampHour(0)+i*3600,100+i)
	}
	for i = 0; i < 24*7; i++ {
		//KVStore.Put("ritesh2",3600+i*3600,100+i)
		//KVStore.Put("ritesh2",6084000+i*3600,100+i)
		KVStore.Put("ritesh2",GetEpochTimestampHour(0)+i*3600,100+i)
	}

	fmt.Println(KVStore.Get("ritesh2"))
	fmt.Println(KVStore.Get("ritesh3"))

	KVStore.Create("ritesh3",3600)
	KVStore.Put("ritesh3",3600,101)
	KVStore.Put("ritesh3",6084000,101)
	//KVStore.Delete("ritesh3")

	fmt.Println("going to sleep")
	time.Sleep(3 * time.Second)
}

func main() {
	KVStore := NewStore("/tmp/data1","/tmp/keys/") // echo "" > /tmp/data1; mkdir -p "/tmp/keys/";
	//KVStore.Restore()
	KVStore.StartStoreExpiredEvictor()
	KVStore.StartStoreDiskFlusher()

	test(KVStore)

	KVStore.Backup()
	KVStore.StopStoreExpiredEvictor()
	KVStore.StopStoreDiskFlusher()
	fmt.Println("Waiting")
	time.Sleep(3 * time.Second)
}
