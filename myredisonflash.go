package main

import "fmt"
import "encoding/json"
import "os"
import "time"
import "sync"
import "hash/crc32"
//import "math/rand"
import "net/http"
import "io/ioutil"
import "net/url"
import "strconv"
import "bufio"

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
	//db map[string]*TimeSeriesRecord
	db sync.Map
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
			x, _ := k.db.Load(keyToFlush)
			timeSeriesRecordPtr = x.(*TimeSeriesRecord)
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

func (k *Store) StopStoreDiskFlusher() {
	k.doneStoreDiskFlusher <- true
}

func StoreExpiredEvictor(k *Store) {
	//ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-k.doneStoreExpiredEvictor:
			fmt.Println("StoreExpiredEvictor stopped.");
			return
		//case t := <-ticker.C:
			//fmt.Println("Eviction triggered at", t)
		}
	}
}

func NewStore(filename string, directoryname string) *Store {
	store := &Store {
		//db: map[string]*TimeSeriesRecord{},
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

func (k *Store) StopStoreExpiredEvictor() {
	k.doneStoreExpiredEvictor <- true
}

func mallocTimeSeriesRecord() *TimeSeriesRecord{
	var t TimeSeriesRecord
	return &t
}

type TimeSeriesResponse struct {
	Key *string
	Timestamp uint64
	Value uint64
}


func (k *Store) GetRange(key string, startTimestamp uint64, endTimestamp uint64) []TimeSeriesResponse {
	timeSeriesRecord := k.Get(key)
	if nil == timeSeriesRecord {
		return nil /* key not present */
	}
	startTimestampHour := GetEpochTimestampHour(startTimestamp)
	endTimestampHour := GetEpochTimestampHour(endTimestamp)

	if endTimestampHour <= timeSeriesRecord.StartTimestampHour {
		return nil /* no data for that much past */
	}

	countHours := ((endTimestampHour-startTimestampHour)/SECONDS_IN_HOUR) + 1
	retTimeSeriesResponse := make([]TimeSeriesResponse, countHours, countHours)
	currTimestampHour := startTimestampHour
	for idx, _ := range retTimeSeriesResponse {
		retTimeSeriesResponse[idx].Key = &key
		retTimeSeriesResponse[idx].Timestamp = currTimestampHour
		currTimestampHour += SECONDS_IN_HOUR
	}

	curr := timeSeriesRecord
	for curr != nil {
		curr_endTimestampHour := (curr.StartTimestampHour + ((DAYS * HOURS) - 1 ) * SECONDS_IN_HOUR)
		if startTimestampHour >= curr.StartTimestampHour &&
			startTimestampHour <= curr_endTimestampHour {
			break
		}
		curr = curr.Next
	}

	if curr == nil {
		return retTimeSeriesResponse
	}
	first := curr

	for curr != nil {
		curr_endTimestampHour := (curr.StartTimestampHour + ((DAYS * HOURS) - 1 ) * SECONDS_IN_HOUR)
		if endTimestampHour >= curr.StartTimestampHour &&
			endTimestampHour <= curr_endTimestampHour {
			break
		}
		curr = curr.Next
	}

	//last := curr

	curr = first
	for curr != nil {
		curr = curr.Next
	}

	return retTimeSeriesResponse
}


func (k *Store) Get(key string) *TimeSeriesRecord {
	k.BigLock.RLock()
	var timeSeriesRecordPtr *TimeSeriesRecord
	x, _ := k.db.Load(key)
	timeSeriesRecordPtr = x.(*TimeSeriesRecord)
	if nil == timeSeriesRecordPtr {
		k.BigLock.RUnlock()
		return nil
	}
	var RAMTimeSeriesRecord TimeSeriesRecord
	timeSeriesRecordPtr.rwLock.RLock()
	RAMTimeSeriesRecord = *timeSeriesRecordPtr
	curr := &RAMTimeSeriesRecord
	next := curr.Next
	for next != nil {
		nextTimeSeriesRecord := mallocTimeSeriesRecord()
		*nextTimeSeriesRecord = *next
		curr.Next = nextTimeSeriesRecord
		curr = curr.Next
		next = curr.Next
	}
	firstDiskTimeSeriesRecord, lastDiskTimeSeriesRecord := k.GetFromDisk(key)
	timeSeriesRecordPtr.rwLock.RUnlock()
	k.BigLock.RUnlock()
	if nil != lastDiskTimeSeriesRecord {
		lastDiskTimeSeriesRecord.Next = &RAMTimeSeriesRecord
		return firstDiskTimeSeriesRecord
	} else {
		return &RAMTimeSeriesRecord
	}
}

// Readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned iff there is an error with the
// buffered reader.
func Readln(r *bufio.Reader) (string, error) {
  var (isPrefix bool = true
       err error = nil
       line, ln []byte
      )
  for isPrefix && err == nil {
      line, isPrefix, err = r.ReadLine()
      ln = append(ln, line...)
  }
  return string(ln),err
}

func (k *Store) GetFromDisk(key string) (firstDiskTimeSeriesRecord *TimeSeriesRecord, lastDiskTimeSeriesRecord *TimeSeriesRecord) {
	f, err := os.Open(k.directoryname + key)
	if err != nil {
		return nil, nil
	}
	r := bufio.NewReader(f)
	json_line, e := Readln(r)
	if e == nil {
		firstDiskTimeSeriesRecord = mallocTimeSeriesRecord()
		lastDiskTimeSeriesRecord = firstDiskTimeSeriesRecord
		json.Unmarshal([]byte(json_line), firstDiskTimeSeriesRecord)
		json_line, e = Readln(r)
	}
	for e == nil {
		lastDiskTimeSeriesRecord.Next = mallocTimeSeriesRecord()
		lastDiskTimeSeriesRecord = lastDiskTimeSeriesRecord.Next
		json.Unmarshal([]byte(json_line), lastDiskTimeSeriesRecord)
		json_line, e = Readln(r)
	}
	return firstDiskTimeSeriesRecord, lastDiskTimeSeriesRecord
}

const CURRENT_TIMESTAMP = 0

func GetEpochTimestampHour(timestamp uint64) uint64 {
	if CURRENT_TIMESTAMP == timestamp {
		timestamp = uint64(time.Now().Unix()) // epoch
	}
	return uint64(timestamp/SECONDS_IN_HOUR)*SECONDS_IN_HOUR  // closest past hour near timestamp
}


func (k *Store) Put(key string, timestamp uint64, value uint64) bool {
	k.BigLock.RLock()
	var timeSeriesRecordPtr *TimeSeriesRecord
	x, _ := k.db.Load(key)
	if nil == x {
		k.BigLock.RUnlock()
		return false // please do Create() before doing Put()
	}
	timeSeriesRecordPtr = x.(*TimeSeriesRecord)
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
		endTimestampHour := (curr.StartTimestampHour + ((DAYS * HOURS) - 1 ) * SECONDS_IN_HOUR)
		if timestampHour <= endTimestampHour {
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

func (k *Store) Create(key string, timestamp uint64) {
	k.BigLock.RLock()
	var newTimeSeriesRecord TimeSeriesRecord
	//newTimeSeriesRecord.StartTimestampHour = GetEpochTimestampHour(timestamp) - (rand.Uint64()  % (DAYS * HOURS * SECONDS_IN_HOUR))
	newTimeSeriesRecord.StartTimestampHour = GetEpochTimestampHour(timestamp)
	var rwLock sync.RWMutex
	newTimeSeriesRecord.rwLock = &rwLock
	k.db.LoadOrStore(key, &newTimeSeriesRecord) /* create timeseries (if not present) */
	k.BigLock.RUnlock()
}

func (k *Store) Delete(key string) {
	k.BigLock.RLock()
	k.db.Delete(key)
	os.Remove(k.directoryname + key)
	k.BigLock.RUnlock()
}

func (k *Store) Backup() {
	k.BigLock.Lock()
	k_db_map := make(map[string]*TimeSeriesRecord)
	k.db.Range(func( k interface{}, v interface{}) bool {
		k_db_map[k.(string)] = v.(*TimeSeriesRecord)
		return true
	})
	mapJSON, _ := json.Marshal(k_db_map)
	file, _ := os.Create(k.filename)
	file.Write(mapJSON)
	file.Close()
	k.BigLock.Unlock()
}

func mallocRwLock() *sync.RWMutex {
	var rwLock sync.RWMutex
	return &rwLock
}
func (k *Store) Restore() { /* should be fired only during startup (before anything else) */
	k.BigLock.Lock()
	file, _ := os.Open(k.filename)
	fileinfo, _ := file.Stat()
	mapJSON := make([]byte, fileinfo.Size())
	file.Read(mapJSON)
	k_db_map := make(map[string]*TimeSeriesRecord)
	json.Unmarshal(mapJSON, &k_db_map)
	file.Close()
	for key, value := range k_db_map {
		value.rwLock = mallocRwLock()
		k.db.Store(key, value)
	}
	k.BigLock.Unlock()
}

func test(KVStore *Store) {
	fmt.Println(KVStore.Get("ritesh2"))
	KVStore.Create("ritesh2",1679295600)
	//fmt.Println(KVStore.Get("ritesh2"))
	//KVStore.Create("ritesh2",1679295600)
	//fmt.Println(KVStore.Get("ritesh2"))
	//KVStore.Put("ritesh2",1679295600,101)
	//KVStore.Put("ritesh2",1679900400,101)
	fmt.Println(KVStore.Get("ritesh2"))

	var i uint64
	for i = 0; i < 24*7; i++ {
		KVStore.Put("ritesh2",1679295600+i*3600,100+i)
		//KVStore.Put("ritesh2",1679900400+i*3600,100+i)
		//KVStore.Put("ritesh2",GetEpochTimestampHour(CURRENT_TIMESTAMP)+i*3600,100+i)
	}
	fmt.Println("checkpoint")
	for i = 0; i < 24*7; i++ {
		//KVStore.Put("ritesh2",1679295600+i*3600,100+i)
		KVStore.Put("ritesh2",1679900400+i*3600,100+i)
		//KVStore.Put("ritesh2",GetEpochTimestampHour(CURRENT_TIMESTAMP)+i*3600,100+i)
	}
	for i = 0; i < 24*7; i++ {
		//KVStore.Put("ritesh2",1679295600+i*3600,100+i)
		//KVStore.Put("ritesh2",1679900400+i*3600,100+i)
		KVStore.Put("ritesh2",GetEpochTimestampHour(CURRENT_TIMESTAMP)+i*3600,100+i)
	}

	fmt.Println(KVStore.Get("ritesh2"))
	fmt.Println(KVStore.Get("ritesh3"))

	KVStore.Create("ritesh3",1679295600)
	KVStore.Put("ritesh3",1679295600,101)
	KVStore.Put("ritesh3",1679900400,101)
	//KVStore.Delete("ritesh3")

	fmt.Println("going to sleep")
	time.Sleep(3 * time.Second)
}

func handleSubmitStat(w http.ResponseWriter, r *http.Request) {
	postData, _ := ioutil.ReadAll(r.Body)
	queryParams, _ := url.ParseQuery(string(postData))
	fmt.Println(queryParams)

	stat := queryParams["stat"]
	if stat == nil ||
		len(stat[0]) == 0 {
		w.WriteHeader(http.StatusBadRequest) // BadRequest
		return
	}

	value := queryParams["value"]
	if value == nil ||
		len(value[0]) == 0 {
		w.WriteHeader(http.StatusBadRequest) // BadRequest
		return
	}

	value_uint64, value_err := strconv.ParseUint(value[0], 10, 64)
	if value_err != nil {
		w.WriteHeader(http.StatusBadRequest) // BadRequest
		return
	}

	KVStore.Create(stat[0], CURRENT_TIMESTAMP)
	KVStore.Put(stat[0], CURRENT_TIMESTAMP, value_uint64)
	w.WriteHeader(http.StatusNoContent) //GoodRequest
}

func handleBackup(w http.ResponseWriter, r *http.Request) {
	KVStore.Backup()
	w.WriteHeader(http.StatusNoContent) //GoodRequest
}

var KVStore *Store

func main() {
	KVStore = NewStore("/tmp/data1","/tmp/keys/") // echo "" > /tmp/data1; mkdir -p "/tmp/keys/";
	//KVStore.Restore()
	//KVStore.StartStoreExpiredEvictor()
	//KVStore.StartStoreDiskFlusher()

	//test(KVStore)
	//t := KVStore.Get("ritesh2")
	//tJSON, _ := json.Marshal(t)
	//fmt.Println(string(tJSON))

	//fmt.Println(KVStore.GetRange("ritesh2",3600,7200))

	http.HandleFunc("/submitstat", handleSubmitStat)
	http.HandleFunc("/backup", handleBackup)
	http.ListenAndServe(":3333", nil)
	//KVStore.StopStoreExpiredEvictor()
	//KVStore.StopStoreDiskFlusher()
	//KVStore.Backup()
	fmt.Println("Waiting")
	time.Sleep(3 * time.Second)
}
