package fs

import (
	"crypto/md5"
	"encoding/base32"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syncfolders/bep"
	"syncfolders/fswatcher"
	"syncfolders/node"
	"time"
)

//todo 对数据库的操作再进行封装，添加重试机制

/**
todo 把具体的计算存储任务分发给每一个folderNode
 进而做到每一个folder 都可以并发
*/

var (
	LocalUser node.DeviceId
)

var (
	STons           int64 = 1000000000
	ErrExistFolder        = errors.New("the folder has exist ")
	ErrWatcherWrong       = errors.New("wrong occur create watcher ")
)

var (
	IgnoredHide = true
)

func init() {
	dir, _ := os.Getwd()
	infos, _ := ioutil.ReadDir(dir)
	for _, info := range infos {
		if filepath.Ext(info.Name()) == DbFileSuff ||
			filepath.Ext(info.Name()) == ".db-journal" {
			filePath := filepath.Join(dir, info.Name())
			_ = os.Remove(filePath)
		}
	}
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
			delete(fs.folders, folderId)
			fs.lock.Unlock()
			return ErrWatcherWrong
		}
		fn.w = w
		err = fn.initNode(randomDbName())
		if err != nil {
			w.Close()
			delete(fs.folders, folderId)
			fs.lock.Unlock()
			return err
		}

		fs.lock.Unlock()
		fs.initFolderIndex(folderId)

		go fs.receiveEvent(folderId)
		go fs.handleEvents(folderId)
		fn.competeInit()
	}
	return nil
}

func randomDbName() string {
	var prefix = strconv.FormatInt(time.Now().Unix(), 10)
	var suffix = strconv.FormatInt(time.Now().UnixNano(), 10)
	var name = prefix + suffix
	var hash = md5.Sum([]byte(name))
	name = base32.StdEncoding.EncodeToString(hash[:])
	return name + DbFileSuff
}

func (fs *FileSystem) RemoveFolder(folder string) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fn, ok := fs.folders[folder]

	if !ok {
		return
	}

	if !fn.waitAvailable() {
		delete(fs.folders, folder)
		return
	}

	_ = fn.Close()
	delete(fs.folders, folder)
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

	fn.calculateIndex()
}

func (fs *FileSystem) GetIndex(folder string) *bep.Index {
	index := new(bep.Index)
	index.Files = make([]*bep.FileInfo, 0)
	index.Folder = folder
	fs.lock.RLock()
	if fn, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		return fn.getIndex()
	} else {
		fs.lock.RUnlock()
		return nil
	}
}

func (fs *FileSystem) GetUpdates(folder string) []*bep.IndexUpdate {
	updates := make([]*bep.IndexUpdate, 0)
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		return f.getUpdatesAfter(f.indexSeq)
	} else {
		fs.lock.RUnlock()
	}

	return updates
}

//GetUpdateAfter 返回某个id 后的文件夹update id 是 indexSeq的字段
func (fs *FileSystem) GetUpdateAfter(folderId string, id int64) []*bep.IndexUpdate {
	fs.lock.RLock()
	if f, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return f.getUpdatesAfter(id)
	} else {
		return nil
	}
}

func (fs *FileSystem) GetIndexSeq(folderId string, id int64) *IndexSeq {
	fs.lock.RLock()
	if f, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return f.getIndexSeq(id)
	} else {
		return nil
	}
}

func (fs *FileSystem) GetIndexSeqAfter(folderId string, id int64) []*IndexSeq {
	fs.lock.RLock()
	if f, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return f.getIndexSeqAfter(id)
	} else {
		return nil
	}
}

func (fs *FileSystem) receiveEvent(folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}

	if !fn.waitAvailable() {
		return
	}

	events := fn.w.Events()

outter:
	for {
		select {
		case <-fn.stop:
			return
		case e, _ := <-events:
			relName, err := filepath.Rel(fn.realPath, e.Name)
			if err != nil {
				continue outter
			}
			/*	if fn.IsBlock(relName) {
					log.Println("discard a event of ",relName)
					continue outter
				}
			*/
			//log.Println(relName," of event pass ")
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
//todo 将处理event 与计算update 的任务分发给 fn

/**
在优化这个模块时 发现一个现象,有的文件出现了两个记录
我推测这是由于文件被记录为 create write
两个事件导致的

对于事件的缓存应该在单独的线程里,

*/
func (fs *FileSystem) handleEvents(folder string) {
	fs.lock.Lock()
	fn, ok := fs.folders[folder]
	fs.lock.Unlock()
	if !ok {
		return
	}

	if !fn.waitAvailable() {
		return
	}

	events := fn.events

	ticker := time.NewTicker(time.Second * 8)
	ticker2 := time.NewTicker(time.Second * 45)
	for {
		select {
		case <-fn.stop:
			ticker.Stop()
			return
		case e, _ := <-events:
			/*relName, err := filepath.Rel(fn.realPath, e.Name)
			if err != nil {
				continue outter
			}
			if fn.IsBlock(relName) {
				continue outter
			}*/
			fn.handleEvent(e)
		case <-ticker.C:
			fn.calculateUpdate()
		case <-ticker2.C:
			fn.needScanner = true
		}
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

/**
GetIndexUpdateMap 根据输入的 indexSeq ，返回对应的
index 和 indexUpdate
*/
//todo 	应该显示得说明folderId
func (fs *FileSystem) GetIndexUpdateMap(folderId string, indexSeqs []*IndexSeq) (map[int64]*bep.Index,
	map[int64]*bep.IndexUpdate) {
	fs.lock.RLock()
	if fn, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return fn.getIndexUpdatesMap(indexSeqs)
	} else {
		fs.lock.RUnlock()
	}
	return nil, nil
}

func (fs *FileSystem) SetIndexSeq(index *IndexSeq) error {
	fs.lock.RLock()
	if fn, ok := fs.folders[index.Folder]; ok {
		fs.lock.RUnlock()
		return fn.setIndexSeq(index)
	} else {
		fs.lock.RUnlock()
	}
	return nil
}

//sync 逻辑中会干预记录操作
/*func (fs *FileSystem) SetFileInfo(folderId string, info *bep.FileInfo) (int64, error) {
	fs.lock.RLock()
	if fn, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return fn.setFile(info)
	} else {
		fs.lock.RUnlock()
	}
	return -1, nil
}


*/

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
	ErrInvalidFile = errors.New("file is invalid")
)

/**
GetData return data block
*/

func (fs *FileSystem) GetData(folder, name string, offset int64, size int32) ([]byte, error) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		if f.isInvalid(name) {
			return nil, ErrInvalidFile
		}
		realPath := f.realPath
		filePath := filepath.Join(realPath, name)
		//log.Printf("get data of %s", filePath)
		fPtr, err := os.Open(filePath)
		if err != nil {
			return nil, ErrNoSuchFile
		}
		defer fPtr.Close()
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
	fs.lock.Lock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.Unlock()
		filePath := filepath.Join(f.realPath, name)
		f.eventSet.discardEventOfFile(filePath)
	} else {
		fs.lock.Unlock()
	}
}

func (fs *FileSystem) getFilePath(folderId, name string) string {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if folder, ok := fs.folders[folderId]; ok {
		return filepath.Join(folder.realPath, name)
	} else {
		return ""
	}
}

func (fs *FileSystem) GetFolderProxy(folderId string) *FolderNodeProxy {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	if folder, ok := fs.folders[folderId]; ok {
		return folder.NewFoldeProxy()
	} else {
		return nil
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

func (fs *FileSystem) DisableCalculateUpdate(folder string) {
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

func (fs *FileSystem) EnableCalculateUpdate(folder string) {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		f.EnableUpdate()
	} else {
		fs.lock.RUnlock()
	}

}

//StartUpdateTransaction
func (fs *FileSystem) StartUpdateTransaction(folder string) bool {
	fn := fs.getFolderNode(folder)
	if fn != nil {
		return fn.startUpdate()
	}
	return false
}

//EndUpdateTransaction
func (fs *FileSystem) EndUpdateTransaction(folder string) {
	fn := fs.getFolderNode(folder)
	if fn != nil {
		fn.endUpdate()
	}
}

func (fs *FileSystem) getFolderNode(folder string) *FolderNode {
	fs.lock.RLock()
	if f, ok := fs.folders[folder]; ok {
		fs.lock.RUnlock()
		return f
	} else {
		fs.lock.RUnlock()
	}

	return nil
}
