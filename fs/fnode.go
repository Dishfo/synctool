package fs

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syncfolders/bep"
	"syncfolders/fswatcher"
	"syncfolders/tools"
)

var (
	ErrInvalidFolder = errors.New("folder is invalid ")
)

const (
	MaxEventQueue = 1024 * 1024
)

/**
todo 每一个folderNode都会有一个
 单独的db实例
*/

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

	dw *dbWrapper

	lock       sync.RWMutex
	updateFlag chan int

	needScanner bool

	events chan WrappedEvent
	ava    chan int
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
	fn.events = make(chan WrappedEvent, MaxEventQueue)
	fn.stop = make(chan int)
	fn.ava = make(chan int)
	fn.updateFlag = make(chan int, 1)
	return fn
}

func (fn *FolderNode) initNode(dbFile string) error {
	fn.fl = newFileList(fn.realPath)
	info, err := os.Stat(fn.realPath)
	if err != nil || !info.IsDir() {
		return ErrInvalidFolder
	}

	fn.dw, err = newDb(dbFile)
	if err != nil {
		return err
	}

	err = initFileList(fn.fl, fn.realPath)
	if err != nil {
		return err
	}
	return nil
}

func (fn *FolderNode) nextCounter() *bep.Counter {
	counter := new(bep.Counter)
	counter.Id = uint64(LocalUser)
	counter.Value = atomic.AddUint64(fn.versionSeq, 1)
	return counter
}

func (fn *FolderNode) shieldFile(name string) {
	fn.lock.Lock()
	defer fn.lock.Unlock()
	fn.blockFiles[name] = true
}

func (fn *FolderNode) unblock(name string) {
	fn.lock.Lock()
	defer fn.lock.Unlock()
	fn.blockFiles[name] = false
}

func (fn *FolderNode) IsBlock(name string) bool {
	fn.lock.RLock()
	defer fn.lock.RUnlock()
	return fn.blockFiles[name]
}

//DisableUpdate will turn off auto caculate udpate function
func (fn *FolderNode) DisableUpdate() {
	fn.lock.Lock()
	defer fn.lock.Unlock()
	fn.disableUpdater = true
}

func (fn *FolderNode) EnableUpdate() {
	fn.lock.Lock()
	defer fn.lock.Unlock()
	fn.disableUpdater = false
}

func (fn *FolderNode) shouldCalculateUpdate() bool {
	fn.lock.RLock()
	defer fn.lock.RUnlock()
	return !fn.disableUpdater
}

func (fn *FolderNode) calculateIndex() {
	files := fn.getFiles()
	fileInfos := make([]*bep.FileInfo, 0)
	index := new(bep.Index)
	version := &bep.Vector{
		Counters: []*bep.Counter{
			{
				Id:    uint64(LocalUser),
				Value: atomic.AddUint64(fn.versionSeq, 1),
			},
		},
	}

	defer tools.MethodExecTime("calculateIndex ")()
	for _, name := range files {
		if isHide(name) {
			continue
		}

		info, err := GenerateFileInfo(name)
		if err != nil {
			log.Println(err)
			continue
		}
		info.Version = version
		info.ModifiedBy = uint64(LocalUser)
		info.Name, _ = filepath.Rel(fn.realPath, name)
		if info.Name == "." {
			log.Println(name)
		}
		fileInfos = append(fileInfos, info)
	}

	index.Folder = fn.folderId
	index.Files = fileInfos

	defer tools.MethodExecTime("store index  ")()
	err := fn.internalStoreIndex(index)
	for err != nil {
		err = fn.internalStoreIndex(index)
	}
}

func isHide(file string) bool {
	if !IgnoredHide {
		return false
	}
	name := filepath.Base(file)
	return strings.HasPrefix(name, ".")
}

func (fn *FolderNode) competeInit() {
	close(fn.ava)
}

func (fn *FolderNode) getFiles() []string {
	fn.lock.RLock()
	defer fn.lock.RUnlock()
	return fn.fl.getItems()
}

//--------------------------------------------------
func initFileList(fl *fileList, root string) error {
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if isHide(info.Name()) {
			continue
		}

		if info.Name() == "." {
			log.Println("在下才疏学浅")
		}

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

//waitAvailable return false if folderNode has stop
func (fn *FolderNode) waitAvailable() bool {
	select {
	case <-fn.stop:
		return false
	default:
		select {
		case <-fn.ava:
			return true
		case <-fn.stop:
			return false
		}
	}
}

func (fn *FolderNode) Close() error {
	fn.lock.Lock()
	defer fn.lock.Unlock()

	if fn.isClose() {
		return nil
	}

	close(fn.stop)
	fn.w.Close()
	return nil
}

func (fn *FolderNode) isClose() bool {
	select {
	case <-fn.stop:
		return true
	default:
		return false
	}
}

func (fn *FolderNode) cacheEvent(e WrappedEvent) {

	select {
	case fn.events <- e:

	case <-fn.stop:
		return
	}
}

func (fn *FolderNode) handleEvent(e WrappedEvent) {
	fn.lock.Lock()
	defer fn.lock.Unlock()

	if !fn.waitAvailable() {
		return
	}

	switch e.Op {
	case fswatcher.REMOVE:
		fn.setFileInvalid(e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.WRITE:
		fn.setFileInvalid(e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.CREATE:
		fn.eventSet.NewEvent(e)
	case fswatcher.MOVE:
		fn.setFileInvalid(e.Name)
		fn.eventSet.NewEvent(e)
	case fswatcher.MOVETO:
		fn.eventSet.NewEvent(e)
	}
}

func (fn *FolderNode) setFileInvalid(name string) {
	_, err := fn.internalSetInvalid(name)
	for err != nil {
		_, err = fn.internalSetInvalid(name)
	}
}

//todo 解决moveTo 产生的隐秘create事件
func (fn *FolderNode) calculateUpdate() {
	if !fn.waitAvailable() {
		return
	}

	if !fn.shouldCalculateUpdate() {
		return
	}

	if !fn.startUpdate() {
		return
	}

	defer fn.endUpdate()

	if fn.needScanner {
		fn.scanFolderTransaction()
		fn.needScanner = false
	} else {
		fn.fileEventTransaction()
	}
}

func (fn *FolderNode) fileEventTransaction() {

	lists := fn.eventSet.AvailableList()
	indexSeq := new(IndexSeq)
	indexSeq.Folder = fn.folderId
	indexSeq.Seq = make([]int64, 0)
	tx, err := fn.dw.GetTx()
	if err != nil {
		return
	}

	defer tools.MethodExecTime("file event udapte ")()
	for _, l := range lists {
		ids, err := newFileInfo(fn, l, tx)
		if err == errDbWrong {
			log.Printf("can't store fileinfo")
			_ = tx.Rollback()
			return
		} else if err == errNoNeedInfo {
			continue
		} else if err != nil {
			log.Panic(err, " unkonw how to process ")
		}
		indexSeq.Seq = append(indexSeq.Seq, ids...)
	}

	_ = tx.Commit()
	defer tools.MethodExecTime("store update seq ")()
	_, err = fn.internalStoreIndexSeq(indexSeq)
	for err != nil {
		log.Panic(err, " unkonw how to process ")
		_, err = fn.internalStoreIndexSeq(indexSeq)
	}
}

//newFileinfo 获取到一段时间内某一名称文件的对应的一系列事件
/**
todo 我们难以恢复一系列动作中文件夹的状态
 唯一可以稳定获取的只有最后一个事件
 对于delete 之后的 create 或　move to 都可以让其失去意义
*/

const (
	newFile = iota
	oldFile
)

const (
	createFile = 0x1000
	removeFile = 0x2000
	editFile   = 0x4000

	createOp = 0x10
	removeOp = 0x20
	writeOp  = 0x30
	moveOp   = 0x40
	moveToOp = 0x50
)

type fileState struct {
	isExist bool
	target  int
	state   int16
}

//用于外部模块设置
func (fn *FolderNode) setFile(info *bep.FileInfo) (int64, error) {
	filele := fn.fl.findFile(info.Name)
	filePath := filepath.Join(fn.realPath, info.Name)
	if filele == nil && !info.Deleted {
		switch info.Type {
		case bep.FileInfoType_DIRECTORY:
			fn.fl.newFolder(filePath)
		default:
			fn.fl.newFile(filePath)
		}
	} else if info.Deleted {
		fn.fl.removeItem(filePath)
	}
	return fn.internalStoreFileInfo(info)
}

//处理文件夹的 move 问题
//类似mkdir -p 对创建的文件夹 不会产生对于子文件夹的监视
func newFileInfo(
	fn *FolderNode,
	l *EventList, tx *sql.Tx) ([]int64, error) {

	var hasMove bool
	var hasMoveTo bool

	baseState := fileState{}
	infoIds := make([]int64, 0)
	event := l.Front()
	laste := event
	folderId := fn.folderId
	name, _ := filepath.Rel(fn.realPath, event.Name)
	filele := fn.fl.findFile(event.Name)

	if filele != nil {
		baseState.target = oldFile
	}

	initState(event.WrappedEvent, &baseState)

	for ; event != nil; event = l.Next(event) {
		laste = event
		if filele != nil &&
			filele.fileType == typeFolder &&
			event.Op == fswatcher.MOVE &&
			baseState.target == oldFile {
			hasMove = true
		}

		if event.Op == fswatcher.MOVETO {
			hasMoveTo = true
		}

		processEvent(event.WrappedEvent, &baseState)
	}
	//从测试来看时间都用于 存储 fileinfo

	if baseState.isExist {

		info, err := GenerateFileInfo(l.Name)
		name, _ := filepath.Rel(fn.realPath, l.Name)
		info.Name = name
		err = fn.appeandFileInfo(info)
		if err != nil {
			return nil, err
		}

		info.ModifiedBy = uint64(LocalUser)
		id, err := bep.StoreFileInfo(tx, fn.folderId, info)
		if err != nil {
			return infoIds, err
		}

		if hasMoveTo {
			infos := generateInfos(l.Name)
			if len(infos) != 0 {
				for _, info := range infos {
					info.Version = &bep.Vector{
						Counters: []*bep.Counter{
							fn.nextCounter(),
						},
					}
					info.Name, _ = filepath.Rel(fn.realPath, info.Name)
					id, err := bep.StoreFileInfo(tx, fn.folderId, info)
					if err != nil {
						return infoIds, err
					}
					infoIds = append(infoIds, id)
				}
			}

			for _, info := range infos {
				fn.onFileCreate(info)
			}
		}

		infoIds = append(infoIds, id)

		if baseState.target == newFile &&
			filele == nil {

			fn.onFileCreate(info)
		}

	} else {
		if filele != nil {
			info, err := fn.generateDelFileInfo(folderId, name, laste.WrappedEvent)
			if err != nil {
				return infoIds, err
			}
			id, err := bep.StoreFileInfo(tx, fn.folderId, info)
			if err != nil {
				return infoIds, err
			}

			infos := make([]*bep.FileInfo, 0)
			if hasMove {
				files := fn.fl.getSubFiles(l.Name)
				for _, f := range files {
					name, _ := filepath.Rel(fn.realPath, f)
					info, err :=
						fn.generateDelFileInfo(folderId, name, laste.WrappedEvent)
					if err != nil {
						return infoIds, err
					}
					id, err := bep.StoreFileInfo(tx, fn.folderId, info)
					if err != nil {
						return infoIds, err
					}
					infoIds = append(infoIds, id)
					infos = append(infos, info)
				}
			}

			for _, info := range infos {
				fn.onFileMove(info)
			}

			fn.onFileDelete(info)
			infoIds = append(infoIds, id)
		}
	}
	l.BackWard(laste)

	return infoIds, nil
}

/**
用于测试使用以树型输出 程序认知中的文件夹子文件构成
*/

func (fn *FolderNode) fileTree() {

}

func initState(we WrappedEvent, state *fileState) {
	switch we.Op {
	case fswatcher.WRITE:
		state.isExist = true
		state.state = editFile | writeOp
	case fswatcher.CREATE:
		state.isExist = true
		state.target = newFile
		state.state = createFile | createOp
	case fswatcher.REMOVE:
		state.isExist = false
		state.target = oldFile
		state.state = removeFile | removeOp
	case fswatcher.MOVETO:
		state.isExist = true
		if state.target == oldFile {
			state.state = editFile | moveToOp
		} else {
			state.state = createFile | moveToOp
		}
	case fswatcher.MOVE:
		state.isExist = false
		state.target = oldFile
		state.state = removeFile | moveOp
	}
}

func generateInfos(dir string) []*bep.FileInfo {
	infos := make([]*bep.FileInfo, 0)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return infos
	}
	for _, f := range files {
		if isHide(f.Name()) {
			continue
		}

		filePath := filepath.Join(dir, f.Name())
		info, err := GenerateFileInfo(filePath)
		info.Name = filePath
		info.ModifiedBy = uint64(LocalUser)
		if err != nil {
			continue
		}
		infos = append(infos, info)
		if f.IsDir() {
			infos = append(infos, generateInfos(filePath)...)
		}
	}

	return infos
}

func processEvent(we WrappedEvent, state *fileState) {
	switch we.Op {
	case fswatcher.WRITE:
		state.state = editFile | writeOp
	case fswatcher.CREATE:
		state.isExist = true
		state.target = newFile
		state.state = createFile | createOp
	case fswatcher.REMOVE:
		state.isExist = false
		state.state = removeFile | removeOp
	case fswatcher.MOVETO:
		state.isExist = true
		if state.target == oldFile && state.isExist {
			state.state = editFile | moveToOp
		} else {
			state.target = newFile
			state.state = createFile | moveToOp
		}
	case fswatcher.MOVE:
		state.isExist = false
		state.state = removeFile | moveOp
	}
}

//todo 获取本地的某个文件的最新fileInfo
/**
todo 需要记录的文件最新　fileInfo ID 的功能 fileInfo
*/
func (fn *FolderNode) generateDelFileInfo(folder, name string,
	we WrappedEvent) (*bep.FileInfo, error) {
	tx, err := fn.dw.GetTx()
	if err != nil {
		return nil, err
	}

	recentInfo, err := bep.GetRecentInfo(tx, folder, name)
	if err != nil {
		return nil, err
	}
	if recentInfo == nil {
		return nil, nil
	}
	_ = tx.Commit()
	recentInfo.Deleted = true
	recentInfo.Size = 0
	recentInfo.Version.Counters = append(recentInfo.Version.Counters, fn.nextCounter())
	recentInfo.ModifiedS = we.Mods
	recentInfo.ModifiedNs = int32(we.ModNs - STons*we.Mods)
	return recentInfo, nil
}

func (fn *FolderNode) appeandFileInfo(info *bep.FileInfo) error {
	tx, err := fn.dw.GetTx()
	if err != nil {
		return err
	}

	recentInfo, err := bep.GetRecentInfo(tx, fn.folderId, info.Name)
	if err != nil {
		return err
	}
	_ = tx.Commit()
	if recentInfo != nil {
		recentInfo.Version.Counters =
			append(recentInfo.Version.Counters, fn.nextCounter())
		info.Version = recentInfo.Version
	} else {
		info.Version = &bep.Vector{
			Counters: []*bep.Counter{},
		}
		info.Version.Counters = append(info.Version.Counters,
			fn.nextCounter())
	}

	return nil
}

func (fn *FolderNode) getIndex() *bep.Index {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	if !fn.waitAvailable() {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when select index seq t on %s",
			err.Error(), fn.folderId)
		return nil
	}

	indexSeq, err := getIndexSeq(tx, fn.indexSeq)
	if err != nil {
		log.Printf("%s when select index seq t on %s",
			err.Error(), fn.folderId)
		_ = tx.Rollback()
		return nil
	}

	if indexSeq == nil {
		return nil
	}

	index, err := getIndex(tx, indexSeq)
	if err != nil {
		log.Printf("%s when select index seq t on %s",
			err.Error(), fn.folderId)
		_ = tx.Rollback()
		return nil
	}
	_ = tx.Commit()
	return index
}

func (fn *FolderNode) getUpdateById(id int64) *bep.IndexUpdate {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	if !fn.waitAvailable() {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when select indexUpdate on %s",
			err.Error(), fn.folderId)
		return nil
	}
	indexSeq, err := getIndexSeq(tx, id)
	if err != nil {
		panic(err)
	}
	update := new(bep.IndexUpdate)
	if indexSeq != nil {
		update.Folder = fn.folderId
		update.Files = make([]*bep.FileInfo, 0)
		for _, s := range indexSeq.Seq {
			info, err := bep.GetInfoById(tx, s)
			if err != nil {
				log.Println(err)
				_ = tx.Rollback()
				return nil
			}
			update.Files = append(update.Files, info)
		}
	}
	_ = tx.Commit()

	return update
}

func (fn *FolderNode) getUpdatesAfter(id int64) []*bep.IndexUpdate {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	if !fn.waitAvailable() {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when select indexUpdate on %s",
			err.Error(), fn.folderId)
		return nil
	}
	indexSeqs, err := getIndexSeqAfter(tx, id, fn.folderId)
	if err != nil {
		panic(err)
	}

	updates := make([]*bep.IndexUpdate, 0)
	for _, seq := range indexSeqs {
		update := new(bep.IndexUpdate)
		update.Folder = fn.folderId
		update.Files = make([]*bep.FileInfo, 0)
		for _, s := range seq.Seq {
			info, err := bep.GetInfoById(tx, s)
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

func (fn *FolderNode) isInvalid(name string) bool {
	tx, err := fn.dw.GetTx()
	if err != nil {
		return false
	}
	defer tx.Commit()
	return bep.IsInvalid(tx, fn.folderId, name)
}

func (fn *FolderNode) getIndexSeq(id int64) *IndexSeq {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	if !fn.waitAvailable() {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when select IndexSeq on %s",
			err.Error(), fn.folderId)
		return nil
	}

	indexSeq, err := getIndexSeq(tx, id)
	if err != nil {
		_ = tx.Rollback()
		return nil
	}
	_ = tx.Commit()

	return indexSeq
}

func (fn *FolderNode) getIndexSeqAfter(id int64) []*IndexSeq {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	if !fn.waitAvailable() {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when select IndexSeqs on %s",
			err.Error(), fn.folderId)
		return nil
	}

	indexSeqs, err := getIndexSeqAfter(tx, id, fn.folderId)
	if err != nil {
		_ = tx.Rollback()
		return nil
	}
	_ = tx.Commit()
	return indexSeqs
}

func (fn *FolderNode) getIndexUpdatesMap(indexSeqs []*IndexSeq) (map[int64]*bep.Index,
	map[int64]*bep.IndexUpdate) {
	fn.lock.RLock()
	defer fn.lock.RUnlock()

	indexs := make(map[int64]*bep.Index)
	updates := make(map[int64]*bep.IndexUpdate)
	if !fn.waitAvailable() {
		return indexs, updates

	}

	tx, err := fn.dw.GetTx()

	if err != nil {
		log.Println(err)
		return indexs, updates
	}

	for _, indexSeq := range indexSeqs {
		if fn.indexSeq == indexSeq.Id {
			index := fn.getIndex()
			if indexSeqs != nil {
				indexs[indexSeq.Id] = index
			}
		} else {
			log.Println("will get indexUpdate ")
			update := fn.getUpdateById(indexSeq.Id)
			if update != nil {
				updates[indexSeq.Id] = update
			}
		}
	}
	_ = tx.Commit()
	return indexs, updates

}

func (fn *FolderNode) setIndexSeq(indexSeq *IndexSeq) error {
	_, err := fn.internalStoreIndexSeq(indexSeq)
	return err
}

//文件操作回调函数==================================================
var addCount = 0

func (fn *FolderNode) onFileCreate(info *bep.FileInfo) {
	filePath := filepath.Join(fn.realPath, info.Name)
	addCount += 1
	if info.Type == bep.FileInfoType_DIRECTORY {
		fn.fl.newFolder(filePath)
	} else {

		fn.fl.newFile(filePath)
	}
}

func (fn *FolderNode) onFileDelete(info *bep.FileInfo) {
	filePath := filepath.Join(fn.realPath, info.Name)
	fn.fl.removeItem(filePath)
}

func (fn *FolderNode) onFileWrite(info *bep.FileInfo) {
	//ignored
}

func (fn *FolderNode) onFileMove(info *bep.FileInfo) {
	fn.onFileDelete(info)
}

func (fn *FolderNode) onFileMoveTo(info *bep.FileInfo) {
	fn.onFileCreate(info)
}

/**
原子数据库操作===========================================================
*/

func (fn *FolderNode) internalSetInvalid(name string) (int64, error) {
	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when set invlid Flag on %s",
			err.Error(), name)
		return -1, err
	}
	id, err := bep.SetInvalid(tx, fn.folderId, name)
	if err != nil {
		_ = tx.Rollback()
		return -1, err
	}
	_ = tx.Commit()
	return id, nil
}

func (fn *FolderNode) internalStoreIndexSeq(indexSeq *IndexSeq) (int64, error) {
	tx, err := fn.dw.GetTx()
	if err != nil {
		log.Printf("%s when ready to store update ", err.Error())
		return -1, err
	}

	if len(indexSeq.Seq) == 0 {
		err = tx.Commit()
		if err != nil {
			log.Printf("%s when calcaulate update ", err.Error())
		}
		return -1, nil
	}
	indexSeq.Folder = fn.folderId
	id, err := storeIndexSeq(tx, *indexSeq)
	if err != nil {
		_ = tx.Rollback()
		log.Printf("%s when record filinfo Seq ", err.Error())
		return -1, err
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("%s when calcaulate update ", err.Error())
		return -1, err
	}
	return id, nil
}

func (fn *FolderNode) internalStoreIndex(index *bep.Index) error {
	if index == nil {
		return nil
	}

	tx, err := fn.dw.GetTx()
	if err != nil {
		return err
	}
	indexSeq := IndexSeq{
		Seq:    []int64{},
		Folder: index.Folder,
	}

	for _, info := range index.Files {
		id, err := bep.StoreFileInfo(tx, fn.folderId, info)
		if err != nil {
			log.Printf("%s when init index ", err.Error())
			_ = tx.Rollback()
			return err
		}
		indexSeq.Seq = append(indexSeq.Seq, id)
	}

	id, err := storeIndexSeq(tx, indexSeq)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	fn.indexSeq = id

	return nil
}

func (fn *FolderNode) internalStoreFileInfo(info *bep.FileInfo) (int64, error) {

	tx, err := fn.dw.GetTx()
	if err != nil {
		return -1, err
	}

	id, err := bep.StoreFileInfo(tx, fn.folderId, info)
	if err != nil {
		_ = tx.Rollback()
		return -1, err
	}

	err = tx.Commit()
	return id, err
}
