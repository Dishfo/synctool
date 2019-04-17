package syncfile

import (
	"bytes"
	"crypto/md5"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
	"time"
)

/**
提供req等待管理
 */

type requestSet struct {
	reqs      []*bep.Request
	reqIds    []int64
	expectNum int

	reqSeqMap map[int64]int
	devIds    []node.DeviceId
	devReqMap map[node.DeviceId][]int64
	absent    *IntSet
	notify    []*node.ConnectionNotification
	resps     map[int64]*bep.Response
	wait      chan int

	lock sync.Mutex
}

func newReqSet(reqs []*bep.Request, devIds []node.DeviceId) *requestSet {
	reqSet := new(requestSet)
	reqSet.reqs = reqs
	reqSet.reqIds = make([]int64, len(reqs))
	reqSet.devIds = devIds
	reqSet.reqSeqMap = make(map[int64]int)
	reqSet.devReqMap = make(map[node.DeviceId][]int64)
	reqSet.wait = make(chan int)
	reqSet.resps = make(map[int64]*bep.Response)
	reqSet.notify = make([]*node.ConnectionNotification, 0)
	reqSet.absent = newIntSet()

	for i, req := range reqs {
		reqSet.reqIds[i] = int64(req.Id)
		dev := devIds[i]
		reqSet.reqSeqMap[int64(req.Id)] = i
		ids, ok := reqSet.devReqMap[dev]
		if !ok {
			ids = make([]int64, 0)
		}
		ids = append(ids, int64(req.Id))
		reqSet.devReqMap[dev] = ids
	}

	reqSet.expectNum = len(reqs)

	return reqSet
}

func (rs *requestSet) IsCompete() bool {
	select {
	case <-rs.wait:
		return true
	default:
		return false
	}
}

func (rs *requestSet) onConnectionDisConnection(id node.DeviceId) bool {
	if rs.IsCompete() {
		return false
	}

	reqIds := rs.devReqMap[id]
	for _, id := range reqIds {
		seq := rs.reqSeqMap[int64(id)]
		rs.absent.Add(seq)
	}

	return rs.absent.Len() == rs.expectNum
}

func (rs *requestSet) GetResponse(id int64) *bep.Response {
	rs.lock.Unlock()
	defer rs.lock.Unlock()
	return rs.resps[id]
}

//加锁
func (rs *requestSet) stop() {
	select {
	case <-rs.wait:
		return
	default:
		close(rs.wait)
	}
}

func (rs *requestSet) present(resp *bep.Response) bool {
	if rs.IsCompete() {
		return false
	}
	if _, ok := rs.resps[int64(resp.Id)]; ok {
		return false
	}

	rs.resps[int64(resp.Id)] = resp
	seq := rs.reqSeqMap[int64(resp.Id)]
	rs.absent.Add(seq)
	return rs.absent.Len() == rs.expectNum
}

type NotificationProvider interface {
	ProvideNotification(remote node.DeviceId) *node.ConnectionNotification
}

type requestWaittingManager struct {
	provider    NotificationProvider
	reqIdMap    map[int64]int64 //用于寻找red属于哪一个集合
	reqSetMap   map[int64]*requestSet
	idGenerator *int64

	lock sync.Mutex
}

func newReqWaitManager(provider NotificationProvider) *requestWaittingManager {
	rwm := new(requestWaittingManager)
	rwm.provider = provider
	rwm.reqSetMap = make(map[int64]*requestSet)
	rwm.reqIdMap = make(map[int64]int64)
	rwm.idGenerator = new(int64)
	*rwm.idGenerator = 0
	return rwm
}

func (rwm *requestWaittingManager) Present(resp *bep.Response) {
	rwm.lock.Lock()

	id := rwm.reqIdMap[int64(resp.Id)]

	if rs, ok := rwm.reqSetMap[id]; !ok {
		rwm.lock.Unlock()
		return
	} else {
		rwm.lock.Unlock()
		rs.lock.Lock()
		defer rs.lock.Unlock()
		if rs.present(resp) {
			rs.stop()
			rwm.removeReqSet(id)
		}
	}
}

func (rwm *requestWaittingManager) NewTranscation(rs *requestSet) chan int {
	id := atomic.AddInt64(rwm.idGenerator, 1)

	for _, reqId := range rs.reqIds {
		rwm.reqIdMap[int64(reqId)] = id
	}

	for k, _ := range rs.devReqMap {
		devId := k
		go func() {
			notif := rwm.provider.ProvideNotification(devId)
			select {
			case <-rs.wait:
				return
			case <-notif.Ready:
				rs.lock.Lock()
				defer rs.lock.Unlock()
				res := rs.onConnectionDisConnection(devId)
				if res {
					rs.stop()
					rwm.removeReqSet(id)
				}
			}
		}()
	}
	return rs.wait
}

func (rwm *requestWaittingManager) removeReqSet(id int64) {
	rwm.lock.Lock()
	defer rwm.lock.Unlock()

	if _, ok := rwm.reqSetMap[id]; !ok {
		return
	} else {
		delete(rwm.reqSetMap, id)
	}
}

func startSyncTranscation(folder *ShareFolder) bool {
	folder.lock.Lock()
	defer folder.lock.Unlock()
	if folder.isUpdating {
		return false
	}
	folder.isUpdating = true
	return true
}

func endSyncTranscation(folder *ShareFolder) {
	folder.lock.Lock()
	defer folder.lock.Unlock()
	folder.isUpdating = false
}

//尝试为	每一个folder 开启同步
func (sm *SyncManager) prepareSync() {
	folders := sm.GetFolders()
	for _, folder := range folders {
		f := folder
		go func() {
			sm.syncFolder(f.Id)
		}()
	}
}

//todo
//todo
//todo (3个todo做标记 ^ __ ^) 没有考虑对于 delete 的同步
func (sm *SyncManager) syncFolder(folderId string) {
	sm.folderLock.Lock()
	if folder, ok := sm.folders[folderId]; !ok {
		sm.folderLock.Unlock()
		return
	} else {
		if !startSyncTranscation(folder) {
			sm.folderLock.Unlock()
			return
		}
		defer endSyncTranscation(folder)
		//切换fsys 的模式使程序在这段时间内不会产生update Seq
		tFiles := sm.caculateNewestFolder(folder)
		blockSet := descBlockSet(tFiles)
		reqs := sm.createRequests(blockSet)
		reqSet := newReqSet(reqs, blockSet.DeviceIds)
		wait := sm.rwm.NewTranscation(reqSet)
		select {
		case <-wait:
			//等待任务compete 暂时还不知道是否添加定时限制
		}
		sm.filitTargetFiles(tFiles, blockSet,
			reqSet)
		//此时block已携带所有可用数据

		infos := make([]int64, 0)
		tx, err := fs.GetTx()
		if err != nil {
			panic(err)
		}

		for _, tFolder := range tFiles.Folders {
			sm.fsys.BlcokFile(tFolder.Folder, tFolder.Name)
			info := sm.doSyncFolder(tFolder)
			if info != nil {
				id, err := fs.StoreFileinfo(tx, tFolder.Folder, info)
				if err != nil {
					infos = append(infos, id)
				}
			}
			sm.fsys.UnBlockFile(tFolder.Folder, tFolder.Name)
		}

		for _, tFile := range tFiles.Files {
			sm.fsys.BlcokFile(tFile.Folder, tFile.Name)
			sm.doSyncFile(tFile, blockSet)
			info := sm.doSyncFolder(tFile)
			if info != nil {
				id, err := fs.StoreFileinfo(tx, tFile.Folder, info)
				if err != nil {
					infos = append(infos, id)
				}
			}
			sm.fsys.UnBlockFile(tFile.Folder, tFile.Name)
		}

		for _, tLink := range tFiles.Links {
			sm.fsys.BlcokFile(tLink.Folder, tLink.Name)
			sm.doSyncLink(tLink)
			info := sm.doSyncFolder(tLink)
			if info != nil {
				id, err := fs.StoreFileinfo(tx, tLink.Folder, info)
				if err != nil {
					infos = append(infos, id)
				}
			}
			sm.fsys.UnBlockFile(tLink.Folder, tLink.Name)
		}

		if len(infos) != 0 {
			_, err := fs.StoreIndexSeq(tx, fs.IndexSeq{
				Folder: folder.Id,
				Seq:    infos,
			})

			if err != nil {
				//由于逻辑有些复杂 此处并不知道该如何处理
				panic(err)
			}

		}
		//恢复fsys的功能1
		_ = tx.Commit()
	}
}

func (sm *SyncManager) LocalId() node.DeviceId {
	id, _ := sm.cn.Ids()
	devId, _ := node.GenerateIdFromString(id)
	return devId
}

func (sm *SyncManager) caculateNewestFolder(folder *ShareFolder) *TargetFiles {
	tf := new(TargetFiles)
	tf.Files = make([]*TargetFile, 0)
	tf.Folders = make([]*TargetFile, 0)
	tf.Links = make([]*TargetFile, 0)

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	receUpdates, err := GetReceiveUpdateAfter(tx, folder.lastUpdate, folder.Id)
	if err != nil {
		panic(err)
	}

	otx, err := fs.GetTx()
	if err != nil {
		panic(err)
	}

	localIndex := sm.getLocalIndex(folder.Id)
	localUpdate := sm.getLocalIndexUpdate(folder.Id)
	_ = otx.Commit()

	fromMap := make(map[string]node.DeviceId)
	fileMap, localMap := caculateFileMap(receUpdates, localIndex, localUpdate, fromMap)

	for name, info := range fileMap {
		if dev, ok := fromMap[name]; ok {
			//表示来自其他节点
			file := new(TargetFile)
			file.Name = info.Name
			file.Dst = info
			file.Folder = folder.Id
			switch info.Type {
			case bep.FileInfoType_FILE:
				tf.Folders = append(tf.Folders, file)
			case bep.FileInfoType_DIRECTORY:
				file.Blocks = compareFilePart(localMap[info.Name],
					info,
					dev,
					folder.Id)
				tf.Files = append(tf.Files, file)
			case bep.FileInfoType_SYMLINK:
				tf.Links = append(tf.Links, file)
			}
		}
	}

	return tf
}

func (sm *SyncManager) getLocalIndex(folderId string) *bep.Index {
	return sm.fsys.GetIndex(folderId)
}

func (sm *SyncManager) getLocalIndexUpdate(folderId string) []*bep.IndexUpdate {
	return sm.fsys.GetUpdates(folderId)
}

func caculateFileMap(receivedUpdates []*ReceiveIndexUpdate,
	localIndex *bep.Index,
	localUpdates []*bep.IndexUpdate,
	fromMap map[string]node.DeviceId) (map[string]*bep.FileInfo, map[string]*bep.FileInfo) {
	fileMap := make(map[string]*bep.FileInfo)
	localMap := make(map[string]*bep.FileInfo)

	for _, info := range localIndex.Files {
		fileMap[info.Name] = info
		localMap[info.Name] = info
	}

	for _, update := range localUpdates {
		for _, info := range update.Files {
			fileMap[info.Name] = info
			localMap[info.Name] = info
		}
	}

	for _, update := range receivedUpdates {
		for _, info := range update.update.Files {
			res := chooseOneInfo(fileMap[info.Name],
				info)
			if res == 1 {
				fileMap[info.Name] = info
				fromMap[info.Name] = update.remote
			}
		}
	}

	return fileMap, localMap
}

//0 local remote 1
func chooseOneInfo(local, remote *bep.FileInfo) int {
	if local == nil {
		return 1
	}

	if isNewer(local, remote) {
		return 1
	}
	return 0
}

func isNewer(local, remote *bep.FileInfo) bool {
	if (local.ModifiedS + int64(local.ModifiedNs)) <
		(remote.ModifiedS + int64(remote.ModifiedNs)) {
		return true
	}

	return false
}

func compareFilePart(local, remote *bep.FileInfo, dev node.DeviceId, folder string) []*FileBlock {
	blockHashMap := make(map[string]*bep.BlockInfo)
	if local != nil {
		for _, b := range local.Blocks {
			hashStr := base64.StdEncoding.EncodeToString(b.Hash)
			blockHashMap[hashStr] = b
		}
	}

	blocks := make([]*FileBlock, 0)

	if remote.Deleted {
		return blocks
	}

	for _, b := range remote.Blocks {
		fb := new(FileBlock)
		fb.Folder = folder
		fb.Name = local.Name

		hashStr := base64.StdEncoding.EncodeToString(b.Hash)
		if binfo, ok := blockHashMap[hashStr]; ok {
			fillFileBlock(fb, binfo)
			fb.From = 0
		} else {
			fillFileBlock(fb, b)
			fb.From = dev
		}
	}

	return blocks
}

func fillFileBlock(fb *FileBlock, b *bep.BlockInfo) {
	fb.Offset = b.Offset
	fb.Size = b.Size
	fb.Hash = make([]byte, len(b.Hash))
	copy(fb.Hash, b.Hash)
}

//block set
type BlockSet struct {
	All          []*FileBlock
	Local        []int
	Remote       []int
	DeviceIds    []node.DeviceId
	reqMap       map[int]int64
	fileBlockMap map[string][]int
	datas        [][]byte
}

func descBlockSet(tfiles *TargetFiles) *BlockSet {
	bs := new(BlockSet)
	bs.All = make([]*FileBlock, 0)
	bs.Remote = make([]int, 0)
	bs.DeviceIds = make([]node.DeviceId, 0)
	bs.Local = make([]int, 0)
	bs.fileBlockMap = make(map[string][]int)
	bs.reqMap = make(map[int]int64)

	i := 0
	for _, file := range tfiles.Files {
		fileBlocks := make([]int, 0)
		for _, b := range file.Blocks {
			bs.All = append(bs.All, b)
			if b.From == 0 {
				bs.Local = append(bs.Local, i)
			} else {
				bs.DeviceIds = append(bs.DeviceIds, b.From)
				bs.Remote = append(bs.Remote, i)
			}
			fileBlocks = append(fileBlocks, i)
			i++
		}
		bs.fileBlockMap[file.Name] = fileBlocks
	}

	bs.datas = make([][]byte, len(bs.All))
	return bs
}

func (sm *SyncManager) createRequest(block *FileBlock) *bep.Request {
	req := new(bep.Request)
	id := atomic.AddInt64(sm.reqIdGenerator, 1)

	req.Id = int32(id)
	req.Name = block.Name
	req.Offset = block.Offset
	req.Folder = block.Folder
	req.Size = block.Size
	req.Hash = make([]byte, len(block.Hash))
	copy(req.Hash, block.Hash)
	req.FromTemporary = false

	return req
}

func (sm *SyncManager) createRequests(blockSet *BlockSet) []*bep.Request {
	reqs := make([]*bep.Request, 0)
	for _, seq := range blockSet.Remote {
		block := blockSet.All[seq]
		req := sm.createRequest(block)
		blockSet.reqMap[seq] = int64(req.Id)
		reqs = append(reqs, req)
	}
	return reqs
}

//过滤掉不可能当前不可能完成的同步
func (sm *SyncManager) filitTargetFiles(tFiles *TargetFiles,
	blockSet *BlockSet,
	reqSet *requestSet) {

	newFiles := make([]*TargetFile, 0)
	for _, file := range tFiles.Files {
		seqs := blockSet.fileBlockMap[file.Name]
		isCompete := true
		for _, seq := range seqs {
			block := blockSet.All[seq]
			if block.From == 0 {
				data := sm.getLocalData(block)
				if data == nil {
					isCompete = false
					break
				} else {
					if validData(block.Hash, data) {
						blockSet.datas[seq] = data
					} else {
						isCompete = false
						break
					}
				}
			} else {
				reqId := blockSet.reqMap[seq]
				resp := reqSet.GetResponse(reqId)
				if resp == nil || resp.Code != bep.ErrorCode_NO_ERROR {
					isCompete = false
					break
				} else {
					if validData(block.Hash, resp.Data) {
						blockSet.datas[seq] = resp.Data
					} else {
						isCompete = false
						break
					}
				}
			}
		}

		if isCompete {
			newFiles = append(newFiles, file)
		}
	}
	tFiles.Files = newFiles
	pretreatTargetFiles(tFiles)
}

type TargetFolders []*TargetFile

//对folder 进行排序

//对folder 进行排序 移除非法的link文件
func pretreatTargetFiles(tFiles *TargetFiles) {

}

func (sm *SyncManager) getLocalData(block *FileBlock) ( []byte) {
	return sm.fsys.GetData(block.Folder, block.Name,
		block.Offset, block.Size)
}

//缺乏对于hash function 一致调用方式
func validData(hash, data []byte) bool {
	h := md5.Sum(data)
	if bytes.Compare(hash, h[:]) != 0 {
		return false
	}
	return true
}

func (sm *SyncManager) doSyncFolder(tFolder *TargetFile) *bep.FileInfo {
	filePath, err := sm.GetRealPath(tFolder.Folder, tFolder.Name)
	if err != nil {
		return nil
	}

	info, err := os.Stat(filePath)

	if os.IsNotExist(err) {
		err := createFolder(filePath)
		if err != nil {
			log.Printf("%s when sync %s create %s\n",
				err.Error(), tFolder.Folder, filePath)
			return nil
		}
	} else {
		if info.IsDir() {
			return nil
		} else {
			if hasNewerFile(info, tFolder.Dst) {
				return nil
 			}
 			_ = os.Remove(filePath)
 			err := createFolder(filePath)
 			if err != nil {
 				log.Printf("%s when sync %s create %s\n",
 					err.Error(), tFolder.Folder, filePath)
 				return nil
 			}
 		}
 	}

 	return tFolder.Dst
}

const (
 	FolderPermission = 0775
)

func hasNewerFile(info os.FileInfo, info1 *bep.FileInfo) bool {
 	if info.ModTime().UnixNano() > (info1.ModifiedS + int64(info1.ModifiedNs)) {
 		return true
 	}
 	return false
}

//穿件出folder
func createFolder(file string) error {
 	return os.Mkdir(file, FolderPermission)
}

func IsLink(info os.FileInfo) bool {
	mode := info.Mode()
	if mode.Perm()&os.ModeSymlink > 0 {
		return true
	}
	return false
}

//todo 需要考虑失败时 文件的恢复
func (sm *SyncManager) doSyncFile(tFile *TargetFile, blockSet *BlockSet) *bep.FileInfo {
	filePath, err := sm.GetRealPath(tFile.Folder, tFile.Name)
	if err != nil {
		return nil
	}
	info, err := os.Stat(filePath)
	create := false
	if os.IsNotExist(err) {
		create = true
	} else {
		if hasNewerFile(info, tFile.Dst) {
			return nil
		}
		if info.IsDir() {
			deleteFolder(info.Name())
			create = true
		} else if IsLink(info) {
			_ = os.Remove(filePath)
			create = true
		}
	}

	tmpFile, err := generateTmpFile(tFile, blockSet)
	if err != nil {
		return nil
	}
	err = createFile(filePath)
	if err != nil {
		if create {
			_ = os.Remove(filePath)
		}
		return nil
	}
	_, err = dupFile(filePath, tmpFile)
	if err != nil {
		if create {
			_ = os.Remove(filePath)
		}
		return nil
	}
	return tFile.Dst
}

//移除一个文件夹 和文件夹下的所有文件 在调用前应该保证确实是一个folder
func deleteFolder(folder string) {
	infos, _ := ioutil.ReadDir(folder)
	for _, info := range infos {
		filePath := filepath.Join(folder, info.Name())
		if info.IsDir() {
			deleteFolder(filePath)
		} else {
			_ = os.Remove(filePath)
		}
	}
	_ = os.Remove(folder)

}

func (sm *SyncManager) doSyncLink(tLink *TargetFile) *bep.FileInfo {

	return tLink.Dst
}

func (sm *SyncManager) GetFolderPath(folderId string) string {
	sm.folderLock.RLock()
	defer sm.folderLock.RUnlock()
	if folder, ok := sm.folders[folderId]; ok {
		return folder.Real
	} else {
		return ""
	}
}

func (sm *SyncManager) GetRealPath(folderId, name string) (string, error) {
	if folder := sm.GetFolderPath(folderId); folder == "" {
		return "", errors.New("invaild folderId")
	} else {
		return filepath.Join(folder, name), nil
	}
}

const (
	tmpPrefix         = "/tmp/"
	tmpFilePermission = 0775
)

//是否需要提高 i/o效率
func generateTmpFile(tFile *TargetFile, blockSet *BlockSet) (string, error) {
	filePath := fmt.Sprintf("%d%s%s",
		time.Now().UnixNano(), tFile.Folder, tFile.Name)
	filePath = base32.StdEncoding.EncodeToString([]byte(filePath))
	filePath = fmt.Sprintf("%s%s", tmpPrefix, filePath)
	fPtr, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR,
		tmpFilePermission)
	if err != nil {
		return "", err
	}
	defer fPtr.Close()
	seqs := blockSet.fileBlockMap[tFile.Name]
	for _, seq := range seqs {
		block := blockSet.datas[seq]
		if block == nil {
			_ = os.Remove(filePath)
			return "", errors.New("data block is not available")
		}
		_, err := fPtr.Write(block)
		if err != nil {
			_ = os.Remove(filePath)
			return "", errors.New("generateTmp file failed ")
		}
	}

	return filePath, nil
}

func dupFile(dst, src string) (int64, error) {
	dstPtr, err := os.OpenFile(dst, os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		return 0, err
	}
	defer dstPtr.Close()
	srcPtr, err := os.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer dstPtr.Close()
	return io.Copy(dstPtr, srcPtr)
}

func createFile(filePath string) error {
	_, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, tmpFilePermission)
	return err
}
