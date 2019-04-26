package fs

import (
	"bytes"
	"crypto/md5"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syncfolders/bep"
	"syncfolders/fswatcher"
	"syncfolders/node"
	"time"
)

var (
	LocalUser node.DeviceId
)

var (
	STons           int64 = 1000000000
	ErrExistFolder        = errors.New("the folder has exist ")
	ErrWatcherWrong       = errors.New("wrong occur create watcher ")
)

func (fn *FolderNode) shouldCaculateUpadte() bool {

	return !fn.disableUpdater
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
func (fs *FileSystem) AddFolder(folderId string, real string) error {
	fs.lock.Lock()
	if _, ok := fs.folders[folderId]; ok {
		fs.lock.Unlock()
		return ErrExistFolder
	} else {
		fn := newFolderNode(folderId, real)
		fs.folders[folderId] = fn
		w, err := fswatcher.NewWatcher(real)
		if err != nil {
			fs.lock.Unlock()
			return ErrWatcherWrong
		}
		fn.w = w
		err = fn.initNode()
		if err != nil {
			return err
		}

		fs.lock.Unlock()
		fs.initFolderIndex(folderId)

		go fs.receiveEvent(folderId)
		go fs.handleEvents(folderId)
	}
	return nil
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
		fs.lock.RUnlock()
		return fn.getFiles()
	} else {
		fs.lock.RUnlock()
		return []string{}
	}
}

//设置  fl 的文件列表
func (fs *FileSystem) initFolderIndex(folderId string) {
	fn := fs.folders[folderId]
	fs.calculateIndex(fn)
}

//calculateIndex 计算出 folder 的初始index 把对应设置当 fnode 中
//此时的index 可能并不一定是最新
func (fs *FileSystem) calculateIndex(fn *FolderNode) {
	if fn == nil {
		return
	}

	indexSeq := IndexSeq{}

	index := fn.calculateIndex()
	for _, info := range index.Files {
		tx, err := GetTx()
		if err != nil {
			panic(err)
		}
		id, err := StoreFileinfo(tx, fn.folderId, info)
		if err != nil {
			log.Printf("%s when init index ", err.Error())
			_ = tx.Rollback()
			continue
		}
		_ = tx.Commit()
		indexSeq.Seq = append(indexSeq.Seq, id)
	}

	tx, err := GetTx()
	if err != nil {
		//handle error
	}
	indexSeq.Folder = fn.folderId
	id, err := StoreIndexSeq(tx, indexSeq)
	if err != nil {
		_ = tx.Rollback()
		panic(err)
	}
	indexSeq.Id = id
	fn.indexSeq = id
	_ = tx.Commit()

}

func (fs *FileSystem) GetIndex(folder string) *bep.Index {
	index := new(bep.Index)
	index.Files = make([]*bep.FileInfo, 0)
	index.Folder = folder
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	tx, err := GetTx()
	if err != nil {
		panic(err)
	}
	if fn, ok := fs.folders[folder]; ok {
		indexSeq, err := GetIndexSeq(tx, fn.indexSeq)
		if err != nil {
			_ = tx.Rollback()
			panic(err)
		}

		if indexSeq == nil {
			_ = tx.Commit()
			return index
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
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {

		fs.lock.RUnlock()
		return GetUpdate(folder, f.indexSeq)
	} else {
		fs.lock.RUnlock()
	}

	return updates
}

func GetUpdate(folder string, id int64) []*bep.IndexUpdate {
	tx, err := GetTx()
	if err != nil {
		panic(err)
	}

	indexSeqs, err := GetIndexSeqAfter(tx, id, folder)
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
	_ = tx.Commit()
	return updates
}

func (fs *FileSystem) receiveEvent(folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}
	//初始化	完成后再进行后续的相关逻辑
	select {
	case <-fn.stop:
		return
	default:
	}

	events := fn.w.Events()

outter:
	for {
		select {
		case <-fn.stop:
			return
		case e, _ := <-events:
			if fn.IsBlock(e.Name) {
				continue outter
			}
			log.Println(e)
			var we WrappedEvent
			we.Event = e
			now := time.Now()
			we.Mods = now.Unix()
			we.ModNs = now.UnixNano()
			fn.cacheEvent(we)
		}
	}
}

//处理event ，或者计算update
func (fs *FileSystem) handleEvents(folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
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
				fs.calculateUpdate(fn)
			}
		}
	}
}

func (fn *FolderNode) cacheEvent(e WrappedEvent) {
	folder := fn.fl.real
	name, _ := filepath.Rel(folder, e.Name)
	if fn.IsBlock(name) {
		return
	}
	select {
	case fn.events <- e:
	default:
	}
}

func setInvalid(folder, name string) {
	tx, err := GetTx()
	if err != nil {
		log.Panicf("%s when receive a write Event on %s",
			err.Error(), name)
	}
	_, err = SetInvaild(tx, folder, name)
	if err != nil {
		_ = tx.Rollback()
		log.Printf("%s set Invaild Flag on %s ", err.Error(), name)
	}
	_ = tx.Commit()
}

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
	case fswatcher.REMOVE:
		setInvalid(folder, e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.WRITE:
		setInvalid(folder, e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.CREATE:

		fn.eventSet.NewEvent(e)
	case fswatcher.MOVE:
		setInvalid(folder, e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.MOVETO:
		fn.eventSet.NewEvent(e)
	}

}

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
func (fs *FileSystem) calculateUpdate(fn *FolderNode) {
	select {
	case <-fn.stop:
		return
	default:
	}

	lists := fn.eventSet.AvailableList()
	indexSeq := new(IndexSeq)
	indexSeq.Folder = fn.fl.folder
	indexSeq.Seq = make([]int64, 0)

	for _, l := range lists {
		ids, err := newFileInfo(fn, l)
		if err == errDbWrong {
			log.Printf("can't store fileinfo")
			return
		} else if err == errNoNeedInfo {
			continue
		}
		indexSeq.Seq = append(indexSeq.Seq, ids...)
	}

	tx, err := GetTx()
	if err != nil {
		log.Panicf("%s when ready to store update ", err.Error())
	}

	if len(indexSeq.Seq) == 0 {
		err = tx.Commit()
		if err != nil {
			log.Printf("%s when calcaulate update ", err.Error())
		}
		return
	}
	_, err = StoreIndexSeq(tx, *indexSeq)
	if err != nil {
		_ = tx.Rollback()
		log.Panicf("%s when record filinfo Seq ", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("%s when calcaulate update ", err.Error())
	}
}

var (
	errNoNeedInfo = errors.New("don't need " +
		"store a new fileinfo ")
	errDbWrong = errors.New("db tx occur some error ")
)

/**
todo
 对于文件夹的move 事件不会产生对应文件夹下子文件的
 移除事件.也不会产生moveTo 文件下的create事件
 当一个文件夹发生move 时我们应该 从 folderNode 中的
 fileList 获取出它所有的子文件并设置 移除fileInfo
 完善fileList 这个field 	将会用于在内存中记录文件节点构成情况

*/

//移除input中的tx
func newFileInfo(
	fn *FolderNode,
	l *EventList) ([]int64, error) {
	var info *bep.FileInfo
	var err error
	var name string

	ids := make([]int64, 0)
	folder := fn.fl.folder
	base := fn.fl.real

	ele := l.Back()
	name = filepath.Join(fn.fl.real, ele.Name)

	if ele.Op == fswatcher.REMOVE ||
		ele.Op == fswatcher.MOVE {
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

	tx, err := GetTx()
	if err != nil {
		panic(err)
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
		_ = tx.Rollback()
		return ids, errDbWrong
	} else {
		id, err := StoreFileinfo(tx, folder, info)
		if err != nil {
			_ = tx.Rollback()
			return ids, errDbWrong
		}

		ids = append(ids, id)
		err = tx.Commit()
		if err == nil {
			l.BackWard(ele)
		} else {
			return nil, errDbWrong
		}
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
			if err == nil {
				indexs[indexSeq.Id] = index
			}
		} else {
			update, err := GetIndexUpdate(tx, indexSeq)
			if err == nil {
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

var (
	ErrNoSuchFile  = errors.New("no such file")
	ErrInvalidSize = errors.New("except size is invalid")
)

func (fs *FileSystem) GetData(folder, name string, offset int64, size int32) ([]byte, error) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		realPath := f.fl.real
		filePath := filepath.Join(realPath, name)
		fPtr, err := os.Open(filePath)
		if err != nil {
			return nil, ErrNoSuchFile
		}
		data := make([]byte, int(size))
		n, err := fPtr.ReadAt(data, offset)
		if err != nil || n != int(size) {
			return nil, ErrInvalidSize
		}
		return data, nil
	} else {
		fs.lock.RUnlock()
		return nil, ErrNoSuchFile
	}
}

//丢弃某一文件下的事件
func (fs *FileSystem) DiscardEvents(folder, name string) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {

		fs.lock.RUnlock()
		list := f.eventSet.lists[folder]
		if list != nil {
			list.Clear()
		}
	} else {
		fs.lock.Unlock()
	}
}

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
	_ = tx.Commit()
	if err != nil {
		resp.Code = bep.ErrorCode_GENERIC
		return resp
	}

	if info == nil {
		resp.Code = bep.ErrorCode_NO_SUCH_FILE
		return resp
	}

	if info.Invalid {
		resp.Code = bep.ErrorCode_INVALID_FILE
		return resp
	}

	b, err := fs.GetData(req.Folder, req.Name,
		req.Offset,
		req.Size)

	if err == ErrNoSuchFile {
		resp.Code = bep.ErrorCode_NO_SUCH_FILE
		return resp
	}

	if err == ErrInvalidSize {
		resp.Code = bep.ErrorCode_GENERIC
		return resp
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

		fs.lock.RUnlock()
		f.DisableUpdate()
	} else {
		fs.lock.RUnlock()
	}
}

func SetLocalId(id node.DeviceId) {
	LocalUser = id
}

func (fs *FileSystem) EnableCaculateUpdate(folder string) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {

		fs.lock.RUnlock()
		f.EnableUpdate()
	} else {
		fs.lock.RUnlock()
	}

}

/**
fileList 相关操作
*/

func (fs *FileSystem) onMoveFile(e WrappedEvent, folder string) {

}

func (fs *FileSystem) onMoveToFile(e WrappedEvent, folder string) {

}

func (fs *FileSystem) onCreateFile(e WrappedEvent, folder string) {

}

func (fs *FileSystem) onDelete(e WrappedEvent, folder string) {

}
