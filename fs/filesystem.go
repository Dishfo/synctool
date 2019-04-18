package fs

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"errors"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syncfolders/bep"
	"syncfolders/node"
	"syncfolders/watcher"
	"time"
)

var (
	LocalUser node.DeviceId
)

var (
	msTons          int64 = 1000000000
	ErrExistFolder        = errors.New("the folder has exist ")
	ErrWatcherWrong       = errors.New("wrong occur create watcher ")
)

/**
TODO 添加手动设置 fileInfo indexUpdate的模式
 在一段时间内可以手动设置 fileinfo 在手动设置updateSeq
 用于同步文件时

func shieldFile(name string)
func Unblock(name string)
todo 对于target 是file
todo 修改event 接收逻辑 与update 生成逻辑
*/

//todo 哈哈哈哈 整个模块的锁策略还有待完善
//todo get folder--file all update

type FolderNode struct {
	fl             *fileList
	w              *watcher.FolderWatcher
	versionSeq     *uint64
	eventId        *int64
	indexSeq       int64
	lastUpdate     int64
	eventSet       *EventSet
	updateIds      []int64
	disableUpdater bool
	blockFiles     map[string]bool
	events         chan WrappedEvent
	stop           chan int //用于标记这个node已失效
}

func (fn *FolderNode) shieldFile(name string) {
	fn.fl.lock.Lock()
	defer fn.fl.lock.Unlock()
	fn.blockFiles[name] = true
}

func (fn *FolderNode) unblock(name string) {
	fn.fl.lock.Lock()
	defer fn.fl.lock.Unlock()
	fn.blockFiles[name] = false
}

func (fn *FolderNode) IsBlock(name string) bool {
	fn.fl.lock.RLock()
	defer fn.fl.lock.RUnlock()
	return fn.blockFiles[name]
}

//DisableUpdate will turn off auto caculate udpate function
func (fn *FolderNode) DisableUpdate() {
	fn.disableUpdater = true
}

//DisableUpdate will turn off auto caculate udpate function
func (fn *FolderNode) EnableUpdate() {
	fn.disableUpdater = false
}

func (fn *FolderNode) shouldCaculateUpadte() bool {
	fn.fl.lock.RLock()
	defer fn.fl.lock.RUnlock()
	return fn.disableUpdater
}

type FileSystem struct {
	folders           map[string]*FolderNode
	lock              sync.RWMutex
	IgnorePermissions bool
}

func NewFileSystem() *FileSystem {
	fs := new(FileSystem)
	fs.folders = make(map[string]*FolderNode)
	return fs
}

func (fn *FolderNode) NextCounter() *bep.Counter {
	counter := new(bep.Counter)
	counter.Id = uint64(LocalUser)
	counter.Value = atomic.AddUint64(fn.versionSeq, 1)
	return counter
}

//AddFolder ....................
func (fs *FileSystem) AddFolder(folder string, real string) error {
	fs.lock.Lock()
	if _, ok := fs.folders[folder]; ok {
		fs.lock.Unlock()
		return ErrExistFolder
	} else {
		fn := newFolderNode(folder)
		fs.folders[folder] = fn
		w, err := watcher.NewWatcher()
		if err != nil {
			fs.lock.Unlock()
			return ErrWatcherWrong
		}
		fn.w = w
		_ = w.SetFolder(real)
		go fs.receiveEvent(folder)
		fn.fl.lock.Lock()
		defer fn.fl.lock.Unlock()
		fs.lock.Unlock()
		fs.initFileList(fn.fl, real)
	}
	return nil
}

func newFolderNode(folder string) *FolderNode {
	fn := new(FolderNode)
	fn.versionSeq = new(uint64)
	*fn.versionSeq = 0
	fn.fl = newFileList(folder)
	fn.stop = make(chan int)
	fn.eventId = new(int64)
	fn.eventSet = NewEventSet()
	fn.updateIds = make([]int64, 0)
	fn.events = make(chan WrappedEvent, 1024)
	return fn
}

func (fs *FileSystem) RemoveFolder(folder string) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fn, ok := fs.folders[folder]
	if ok {
		close(fn.stop)
		delete(fs.folders, folder)
	}
}

//GetFileList 获取一个文件下的所有文件名
func (fs *FileSystem) GetFileList(folder string) []string {
	fs.lock.RLock()
	if fn, ok := fs.folders[folder]; ok {
		select {
		case <-fn.fl.ready:
		}
		fn.fl.lock.RLock()
		defer fn.fl.lock.RUnlock()
		fs.lock.RUnlock()
		return fn.fl.getItems()
	} else {
		fs.lock.RUnlock()
		return []string{}
	}
}

//设置  fl 的文件列表
func (fs *FileSystem) initFileList(fl *fileList, real string) {
	select {
	case _, ok := <-fl.ready:
		if !ok {
			return
		}
	default:
	}

	fl.items = make(map[string]int)
	fl.real = real
	files := getRealFileList(real)
	for _, f := range files {
		fl.items[f] = 1
	}

	fs.caculateIndex(fl.folder)
	close(fl.ready)
}

//caculateIndex 计算出 folder 的初始index 把对应设置当 fnode 中
func (fs *FileSystem) caculateIndex(folder string) IndexSeq {
	fn := fs.folders[folder]
	indexSeq := IndexSeq{}
	base := fn.fl.real
	items := fn.fl.getItems()
	version := &bep.Vector{
		Counters: []*bep.Counter{
			{
				Id:    uint64(LocalUser),
				Value: atomic.AddUint64(fn.versionSeq, 1),
			},
		},
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf(" %s when prepare generate index ", err.Error())
	}
	for _, item := range items {
		info, err := GenerateFileInfo(filepath.Join(base, item))
		if err != nil {
			log.Panicf("%s when init index ", err.Error())
			continue
		} else {
			info.Version = version
			info.ModifiedBy = uint64(LocalUser)
			info.Name = item
			id, err := StoreFileinfo(tx, folder, info)
			if err != nil {
				log.Panicf("%s when init index ", err.Error())
				continue
			}
			indexSeq.Seq = append(indexSeq.Seq, id)
		}
	}

	indexSeq.Folder = folder
	id, err := StoreIndexSeq(tx, indexSeq)
	if err != nil {
		_ = tx.Rollback()
		panic(err)
	}
	indexSeq.Id = id
	fn.indexSeq = id
	_ = tx.Commit()
	return indexSeq
}

func (fs *FileSystem) GetIndex(folder string) *bep.Index {
	index := new(bep.Index)
	index.Files = make([]*bep.FileInfo, 0)
	index.Folder = folder
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	if fn, ok := fs.folders[folder]; ok {
		indexSeq, err := GetIndexSeq(tx, fn.indexSeq)
		if err != nil {
			_ = tx.Rollback()
			panic(err)
		}
		for _, n := range indexSeq.Seq {
			info, err := GetInfoById(tx, n)
			if err != nil {
				_ = tx.Rollback()
				panic(err)
			}
			index.Files = append(index.Files, info)
		}
	} else {
		_ = tx.Commit()
		return nil
	}
	_ = tx.Commit()
	return index
}

func (fs *FileSystem) GetUpdates(folder string) []*bep.IndexUpdate {
	updates := make([]*bep.IndexUpdate, 0)
	fs.lock.RUnlock()
	defer fs.lock.RUnlock()

	return updates
}

func GetUpdate(folder string) []*bep.IndexUpdate {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	indexSeqs, err := GetIndexSeqAfter(tx, 1, folder)
	if err != nil {
		panic(err)
	}

	updates := make([]*bep.IndexUpdate, 0)
	for _, seq := range indexSeqs {
		update := new(bep.IndexUpdate)
		update.Folder = folder
		update.Files = make([]*bep.FileInfo, 0)
		for _, s := range seq.Seq {
			info, err := GetInfoById(tx, s)
			if err != nil {
				_ = tx.Rollback()
				return updates
			}
			update.Files = append(update.Files, info)
		}

		updates = append(updates, update)
	}

	return updates
}

//todo 等待锁相关操作
func (fs *FileSystem) receiveEvent(folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}
	//初始化	完成后再进行后续的相关逻辑
	select {
	case <-fn.fl.ready:
	}

	events := fn.w.Events()
	ticker := time.NewTicker(time.Second * 5)

outter:
	for {
		select {
		case <-fn.stop:
			ticker.Stop()
			return
		case e, _ := <-events:
			if fn.IsBlock(e.Name) {
				continue outter
			}
			var we WrappedEvent
			we.Event = e
			now := time.Now()
			we.Mods = now.Unix()
			we.ModNs = now.UnixNano()
		case <-ticker.C:
			if fn.shouldCaculateUpadte() {
				fs.caculateUpdate(fn)
			}
		}
	}
}

//处理event ，或者计算update
func (fs *FileSystem) handleEvents(e fsnotify.Event, folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}
	//初始化	完成后再进行后续的相关逻辑
	select {
	case <-fn.fl.ready:
	}

	events := fn.events
	ticker := time.NewTicker(time.Second * 5)

	for {
		select {
		case <-fn.stop:
			ticker.Stop()
			return
		case e, _ := <-events:
			fs.handleEvent(e, folder)
		case <-ticker.C:
			if fn.shouldCaculateUpadte() {
				fs.caculateUpdate(fn)
			}
		}
	}
}

func (fn *FolderNode) cacheEvent(e WrappedEvent) {
	select {
	case fn.events <- e:
	default:
	}
}

func setInvaild(folder, name string) {
	tx, err := db.Begin()
	if err != nil {
		log.Panicf("%s when receive a write Event on %s",
			err.Error(), name)
	}
	_, err = SetInvaild(tx, folder, name)
	if err != nil {
		log.Printf("%s set Invaild Flag on %s ", err.Error(), name)
	}
	_ = tx.Commit()
}

//todo 加锁以确保线程安全 好丑啊
func (fs *FileSystem) handleEvent(e WrappedEvent, folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}
	name, _ := filepath.Rel(fn.fl.real, e.Name)

	e.Name = name
	switch e.Op {
	case fsnotify.Remove:
		setInvaild(folder, e.Name)
		fn.eventSet.NewEvent(e)
	case fsnotify.Write:
		setInvaild(folder, e.Name)
		fn.eventSet.NewEvent(e)
	case fsnotify.Create:
		fn.fl.newItem(e.Name)
		fn.eventSet.NewEvent(e)
	case fsnotify.Rename:
		delE := WrappedEvent{
			Mods:  e.Mods,
			ModNs: e.ModNs,
			Event: e.Event,
		}
		fn.eventSet.NewEvent(delE)
		fn.eventSet.NewEvent(e)
	}
}

/*func setDeleteInfo(tx *sql.Tx,
	fn *FolderNode, folder, name string) int64 {

	version, err := GetRecentVersion(tx, folder, name)
	if err != nil {
		panic(err)
	}

	v := fn.NextCounter()
	now := time.Now()
	info := new(bep.FileInfo)
	info.Name = name
	info.Deleted = true
	info.ModifiedBy = uint64(LocalUser)
	info.ModifiedS = now.Unix()
	info.ModifiedNs = int32(now.UnixNano() - msTons*info.ModifiedS)
	info.Blocks = []*bep.BlockInfo{}
	info.Version = &bep.Vector{
		Counters: append(version.Counters, v),
	}
	id, err := StoreFileinfo(tx, folder, info)
	if err != nil {
		panic(err)
	}
	return id
}
*/

func (fs *FileSystem) findRenameFile(folder string) string {
	fn, ok := fs.folders[folder]
	if !ok {
		return ""
	}

	items := fs.GetFileList(folder)
	files := getRealFileList(fn.fl.real)
	m := make(map[string]bool)
	for _, i := range files {
		m[i] = true
	}
	for _, i := range items {
		if !m[i] {
			return i
		}
	}
	return ""
}

//根	据fodeNode
func (fs *FileSystem) caculateUpdate(fn *FolderNode) {
	select {
	case <-fn.stop:
		return
	default:
	}
	lists := fn.eventSet.AvailableList()
	indexSeq := new(IndexSeq)
	indexSeq.Folder = fn.fl.folder
	indexSeq.Seq = make([]int64, 0)

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("%s when ready to store update ", err.Error())
	}

	for _, l := range lists {
		ids, err := newFileInfo(fn, tx, l)
		if err == errDbWrong {
			_ = tx.Rollback()
			return
		} else if err == errNoNeedInfo {
			continue
		}
		indexSeq.Seq = append(indexSeq.Seq, ids...)
	}

	if len(indexSeq.Seq) == 0 {
		_ = tx.Commit()
		return
	}
	_, err = StoreIndexSeq(tx, *indexSeq)
	if err != nil {
		_ = tx.Rollback()
		log.Panicf("%s when record filinfo Seq ", err.Error())
	}
	_ = tx.Commit()
}

var (
	errNoNeedInfo = errors.New("don't need " +
		"store a new fileinfo ")
	errDbWrong = errors.New("db tx occur some error ")
)

//根据事件队列产生多个info
func newFileInfo(
	fn *FolderNode,
	tx *sql.Tx,
	l *EventList) ([]int64, error) {
	var info *bep.FileInfo
	var err error
	var name string
	ids := make([]int64, 0)
	folder := fn.fl.folder
	base := fn.fl.real

	ele := l.Back()
	name = filepath.Join(fn.fl.real, ele.Name)

	if ele.Op == fsnotify.Remove {
		info, err = GernerateFileInfoInDel(fn.fl.folder,
			ele.Name, ele.WrappedEvent)
		if err != nil {
			return ids, errNoNeedInfo
		}
	} else {
		info, err = GenerateFileInfo(name)
		if err != nil {
			return ids, errNoNeedInfo
		}
	}

	version, err := GetRecentVersion(tx, folder, ele.Name)
	if err != nil {
		log.Panicf("%s when get recent version", err.Error())
	}

	version.Counters =
		append(version.Counters, fn.NextCounter())

	info.Version = version
	info.Name, _ = filepath.Rel(base, name)
	if err != nil {
		log.Printf("%s when genreate fileInfo for "+
			"%s", err.Error(), name)
		return ids, errDbWrong
	} else {
		id, err := StoreFileinfo(tx, folder, info)
		if err != nil {
			return ids, errDbWrong
		}
		l.BackWard(ele)
		ids = append(ids, id)
		return ids, nil
	}
}

func getRealFileList(base string) []string {
	files := make([]string, 0)
	var wg sync.WaitGroup
	var lock sync.Mutex
	infos, err := ioutil.ReadDir(base)
	if err != nil {
		return files
	}

	for _, info := range infos {
		lock.Lock()
		files = append(files, info.Name())
		lock.Unlock()
		if info.IsDir() {
			wg.Add(1)
			tinfo := info
			go func() {
				subs := getRealFileList1(base, tinfo.Name())
				lock.Lock()
				files = append(files, subs...)
				lock.Unlock()
				wg.Done()
			}()
		}
	}

	wg.Wait()
	return files
}

func getRealFileList1(base, parent string) []string {
	files := make([]string, 0)
	name := filepath.Join(base, parent)
	infos, err := ioutil.ReadDir(name)
	if err != nil {
		log.Println(parent, err)
		return files
	}

	for _, info := range infos {
		files = append(files,
			filepath.Join(parent, info.Name()))
		if info.IsDir() {
			subs := getRealFileList1(base,
				filepath.Join(parent, info.Name()))
			files = append(files, subs...)
		}
	}
	return files
}

/**
GetIndexUpdates 根据输入的 indexSeq ，返回对应的
index 和 indexUpdate
*/
func (fs *FileSystem) GetIndexUpdates(indexSeqs []*IndexSeq) (map[int64]*bep.Index,
	map[int64]*bep.IndexUpdate) {
	indexs := make(map[int64]*bep.Index)
	updates := make(map[int64]*bep.IndexUpdate)
	tx, err := GetTx()
	if err != nil {
		return indexs, updates
	}
	for _, indexSeq := range indexSeqs {
		if fs.getFolderIndexId(indexSeq.Folder) == indexSeq.Id {
			index, err := GetIndex(tx, indexSeq)
			if err != nil {
				indexs[indexSeq.Id] = index
			}
		} else {
			update, err := GetIndexUpdate(tx, indexSeq)
			if err != nil {
				updates[indexSeq.Id] = update
			}
		}
	}
	_ = tx.Commit()
	return indexs, updates
}

func (fs *FileSystem) getFolderIndexId(folderId string) int64 {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	if f, ok := fs.folders[folderId]; ok {
		return f.indexSeq
	}
	return -1
}

//todo 介个函数还没有实现
//todo
//todo

func (fs *FileSystem) GetData(folder, name string, offset int64, size int32) []byte {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	if f, ok := fs.folders[folder]; ok {
		realPath := f.fl.real
		filePath := filepath.Join(realPath, name)
		fPtr, err := os.Open(filePath)
		if err != nil {
			return nil
		}
		data := make([]byte, int(size))
		n, err := fPtr.ReadAt(data, offset)
		if err != nil || n != int(size) {
			return nil
		}
		return data
	}
	return nil
}

//todo 函数流程简化 修改函数参数 ，变为去读 folder -- >name ->offset --> size data
func (fs *FileSystem) GetBlock(req *bep.Request) *bep.Response {
	resp := new(bep.Response)
	resp.Id = req.Id
	file := fs.getFilePath(req)

	if file == "" {
		resp.Data = nil
		resp.Code = bep.ErrorCode_NO_SUCH_FILE
		return resp
	}

	tx, err := GetTx()
	if err != nil {
		panic(err)
	}

	info, err := GetRecentInfo(tx, req.Folder, req.Name)
	if err != nil {
		resp.Code = bep.ErrorCode_GENERIC
		return resp
	}

	if info == nil {
		resp.Code = bep.ErrorCode_NO_SUCH_FILE
		return resp
	}

	if !info.Invalid {
		resp.Code = bep.ErrorCode_INVALID_FILE
		return resp
	}

	f, err := os.Open(file)
	//todo 判断文件存在的方法有误
	if !os.IsNotExist(err) {
		resp.Code = bep.ErrorCode_NO_SUCH_FILE
		return resp
	}

	b := make([]byte, req.Size)
	if err != nil {
		n, err := f.ReadAt(b, req.Offset)
		if err != nil || n != int(req.Size) {
			resp.Code = bep.ErrorCode_GENERIC
			return resp
		}
	}

	hash := md5.Sum(b)
	if bytes.Compare(hash[:], req.Hash) != 0 {
		resp.Code = bep.ErrorCode_GENERIC
		return resp
	}
	resp.Data = b
	return resp
}

func (fs *FileSystem) getFilePath(req *bep.Request) string {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if folder, ok := fs.folders[req.Folder]; ok {
		return filepath.Join(folder.fl.real, req.Name)
	} else {
		return ""
	}
}

func (fs *FileSystem) BlockFile(folder, name string) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if fn, ok := fs.folders[folder]; ok {
		fn.shieldFile(name)
	}
}

func (fs *FileSystem) UnBlockFile(folder, name string) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if fn, ok := fs.folders[folder]; ok {
		fn.unblock(name)
	}
}

func (fs *FileSystem) DisableCaculateUpdate(folder string) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		f.fl.lock.Lock()
		defer f.fl.lock.Unlock()
		fs.lock.RUnlock()
		f.DisableUpdate()
	} else {
		fs.lock.RUnlock()
	}
}

func (fs *FileSystem) EnableCaculateUpdate(folder string) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		f.fl.lock.Lock()
		defer f.fl.lock.Unlock()
		fs.lock.RUnlock()
		f.EnableUpdate()
	} else {
		fs.lock.RUnlock()
	}

}
