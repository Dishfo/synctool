package fs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"syncfolders/bep"
	"syncfolders/fswatcher"
)

var (
	ErrInvalidFolder = errors.New("folder is invalid ")
)

type FolderNode struct {
	folderId string
	realPath string

	fl         *fileList
	w          *fswatcher.FolderWatcher
	versionSeq *uint64
	indexSeq   int64

	eventSet       *EventSet
	disableUpdater bool
	blockFiles     map[string]bool

	events chan WrappedEvent
	stop   chan int //用于标记这个node已失效
}

func newFolderNode(folderId string, real string) *FolderNode {
	fn := new(FolderNode)
	fn.folderId = folderId
	fn.realPath = filepath.Clean(real)
	fn.versionSeq = new(uint64)
	*fn.versionSeq = 0

	fn.eventSet = NewEventSet()
	fn.blockFiles = make(map[string]bool)
	fn.events = make(chan WrappedEvent, 1024)
	return fn
}

func (fn *FolderNode) initNode() error {
	fn.fl = newFileList(fn.realPath)
	info, err := os.Stat(fn.realPath)
	if err != nil || !info.IsDir() {
		return ErrInvalidFolder
	}

	err = initFileList(fn.fl, fn.realPath)
	if err != nil {
		return err
	}
	return nil
}

//todo 修改锁策略
func (fn *FolderNode) shieldFile(name string) {
	fn.blockFiles[name] = true
}

func (fn *FolderNode) unblock(name string) {
	fn.blockFiles[name] = false
}

func (fn *FolderNode) IsBlock(name string) bool {

	return fn.blockFiles[name]
}

//DisableUpdate will turn off auto caculate udpate function
func (fn *FolderNode) DisableUpdate() {
	fn.disableUpdater = true
}

func (fn *FolderNode) EnableUpdate() {
	fn.disableUpdater = false
}

func (fn *FolderNode) calculateIndex() *bep.Index {
	files := fn.getFiles()
	fileInfos := make([]*bep.FileInfo, 0)
	index := new(bep.Index)
	for _, name := range files {
		info, err := generateFileInfo(name)
		if err != nil {
			continue
		}
		info.Name, _ = filepath.Rel(fn.realPath, name)
		fileInfos = append(fileInfos, info)
	}

	index.Folder = fn.folderId
	index.Files = fileInfos

	return index
}

func (fn *FolderNode) getFiles() []string {
	return fn.fl.getItems()
}

//--------------------------------------------------
func initFileList(fl *fileList, root string) error {
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}
	for _, info := range infos {
		filePath := filepath.Join(root, info.Name())
		if info.IsDir() {
			fl.newFolder(filePath)
			err = initFileList(fl, filePath)
			if err != nil {
				return err
			}
		} else {
			fl.newFile(filePath)
		}
	}

	return nil
}
