package syncfile

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
)

/**
提供req等待管理
*/
//todo 调整部分函数所处的位置
//todo 寻找时间过长的 tx - __ -
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
	lock      sync.Mutex
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
	rs.lock.Lock()
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

type requestWaitingManager struct {
	provider    NotificationProvider
	reqIdMap    map[int64]int64 //用于寻找red属于哪一个集合
	reqSetMap   map[int64]*requestSet
	idGenerator *int64
	lock        sync.Mutex
}

func newReqWaitManager(provider NotificationProvider) *requestWaitingManager {
	rwm := new(requestWaitingManager)
	rwm.provider = provider
	rwm.reqSetMap = make(map[int64]*requestSet)
	rwm.reqIdMap = make(map[int64]int64)
	rwm.idGenerator = new(int64)
	*rwm.idGenerator = 0
	return rwm
}

func (rwm *requestWaitingManager) Present(resp *bep.Response) {
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

func (rwm *requestWaitingManager) NewTransaction(rs *requestSet) chan int {
	id := atomic.AddInt64(rwm.idGenerator, 1)

	for _, reqId := range rs.reqIds {
		rwm.reqIdMap[int64(reqId)] = id
	}
	rwm.reqSetMap[id] = rs
	for k := range rs.devReqMap {
		devId := k
		go func() {
			notify := rwm.provider.ProvideNotification(devId)
			select {
			case <-rs.wait:
				return
			case <-notify.Ready:
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
	if rs.expectNum == 0 {
		close(rs.wait)
	}
	return rs.wait
}

func (rwm *requestWaitingManager) removeReqSet(id int64) {
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
//todo (3个todo做标记 ^ __ ^)
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
		sm.fsys.DisableCalculateUpdate(folderId)
		sm.folderLock.Unlock()

		defer sm.fsys.EnableCalculateUpdate(folderId)
		defer endSyncTranscation(folder)
		tFiles, last := sm.calculateNewestFolder(folder)

		if folder.ReadOnly {
			return
		}

		defer func() {
			folder.lock.Lock()
			defer folder.lock.Unlock()
			if last > 0 {
				folder.lastUpdate = last
			}
		}()

		if tFiles == nil {
			return
		}

		blockSet := descBlockSet(tFiles)
		reqs := sm.createRequests(blockSet)
		reqSet := newReqSet(reqs, blockSet.DeviceIds)
		wait := sm.rwm.NewTransaction(reqSet)
		logStruct(reqSet.reqs)
		logStruct(reqSet.expectNum)
		go func() {
			for i, req := range reqs {
				remote := blockSet.DeviceIds[i]
				err := sm.SendMessage(remote, req)
				if err != nil {
					log.Printf("%s when request data %d %q",
						err.Error(),
						i,
						req)
					sm.DisConnection(remote)
				}
			}
		}()

		log.Println("WAIT")
		select {
		case <-wait:
		}
		log.Println("resp present")
		logStruct(reqSet.resps)

		sm.filterTargetFiles(tFiles, blockSet,
			reqSet)

		infos := make([]int64, 0)
		for _, tFolder := range tFiles.Folders {
			sm.fsys.BlockFile(tFolder.Folder, tFolder.Name)
			info := sm.doSyncFolder(tFolder)
			if info != nil {
				id, err := sm.fsys.SetFileInfo(folderId, info)
				if err != nil {
					infos = append(infos, id)
				}
			}
			sm.fsys.UnBlockFile(tFolder.Folder, tFolder.Name)
		}

		for _, tFile := range tFiles.Files {
			logStruct(tFile)
			sm.fsys.BlockFile(tFile.Folder, tFile.Name)
			info := sm.doSyncFile(tFile, blockSet)
			if info != nil {
				id, err := sm.fsys.SetFileInfo(folderId, info)
				if err == nil {
					infos = append(infos, id)
				} else {
					log.Printf("%s when store new fileinfo",
						err.Error())
				}
			}
			sm.fsys.UnBlockFile(tFile.Folder, tFile.Name)
		}

		for _, tLink := range tFiles.Links {
			sm.fsys.BlockFile(tLink.Folder, folderId)
			info := sm.doSyncLink(tLink)
			if info != nil {
				id, err := sm.fsys.SetFileInfo(tLink.Folder, info)
				if err != nil {
					infos = append(infos, id)
				}
			}
			sm.fsys.UnBlockFile(tLink.Folder, tLink.Name)
		}

		repeatCount := 15
		err := sm.storeFileInfos(infos, folderId)
		for ; err != nil && repeatCount > 0; repeatCount-- {
			err = sm.storeFileInfos(infos, folderId)
		}

		if repeatCount == 0 {
			log.Printf("store index Seq failed")
		}
	}
}

//TODO　修改这个函数
func (sm *SyncManager) storeFileInfos(infoIds []int64, folder string) error {
	indexSeq := fs.IndexSeq{
		Folder: folder,
		Seq:    infoIds,
	}
	return sm.fsys.SetIndexSeq(&indexSeq)
}

func (sm *SyncManager) LocalId() node.DeviceId {
	id, _ := sm.cn.Ids()
	devId, _ := node.GenerateIdFromString(id)
	return devId
}

//todo 使用额外的标记 来标记某些update已经处理计算过
func (sm *SyncManager) calculateNewestFolder(folder *ShareFolder) (*TargetFiles, int64) {
	tf := new(TargetFiles)
	tf.Files = make([]*TargetFile, 0)
	tf.Folders = make([]*TargetFile, 0)
	tf.Links = make([]*TargetFile, 0)
	var last int64 = 0
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		log.Printf("%s when prepare calculate update of %s ",
			err.Error(), folder.Id)
		return nil, -1
	}

	receiveUpdates, err := getReceiveUpdateAfter(tx, folder.lastUpdate, folder.Id)
	_ = tx.Commit()
	if err != nil {
		log.Printf("%s when prepare get received update of %s ",
			err.Error(), folder.Id)
		return nil, -1
	}
	log.Println("has receive ", len(receiveUpdates))
	if len(receiveUpdates) == 0 {
		return nil, -1
	} else {
		last = receiveUpdates[len(receiveUpdates)-1].Id
	}

	if err != nil {
		log.Printf("%s when prepare get local indexUpdate ",
			err.Error())
		return nil, -1
	}

	localIndex := sm.getLocalIndex(folder.Id)
	localUpdate := sm.getLocalIndexUpdate(folder.Id)

	fromMap := make(map[string]node.DeviceId)
	fileMap, localMap := calculateFileMap(receiveUpdates, localIndex,
		localUpdate, fromMap)

	for name, info := range fileMap {
		log.Println(name, info)
		if dev, ok := fromMap[name]; ok {
			file := new(TargetFile)
			file.Name = info.Name
			file.Dst = info
			file.Folder = folder.Id
			switch info.Type {
			case bep.FileInfoType_DIRECTORY:
				tf.Folders = append(tf.Folders, file)
			case bep.FileInfoType_FILE:
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

	return tf, last
}

func (sm *SyncManager) getLocalIndex(folderId string) *bep.Index {
	return sm.fsys.GetIndex(folderId)
}

func (sm *SyncManager) getLocalIndexUpdate(folderId string) []*bep.IndexUpdate {
	return sm.fsys.GetUpdates(folderId)
}

func calculateFileMap(receivedUpdates []*ReceiveIndexUpdate,
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

	for _, u := range receivedUpdates {
		for _, info := range u.update.Files {
			log.Println("remote ", u.remote, info)
			res := chooseOneInfo(fileMap[info.Name],
				info)
			log.Println(res)
			if res == 1 {
				fileMap[info.Name] = info
				fromMap[info.Name] = u.remote
			}
		}
	}
	return fileMap, localMap
}

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
	return (local.ModifiedS*fs.STons + int64(local.ModifiedNs)) <
		(remote.ModifiedS*fs.STons + int64(remote.ModifiedNs))
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
		fb.Name = remote.Name
		hashStr := base64.StdEncoding.EncodeToString(b.Hash)
		if binfo, ok := blockHashMap[hashStr]; ok {
			fillFileBlock(fb, binfo)
			fb.From = 0
		} else {
			fillFileBlock(fb, b)
			fb.From = dev
		}
		blocks = append(blocks, fb)
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

func descBlockSet(tFiles *TargetFiles) *BlockSet {
	bs := new(BlockSet)
	bs.All = make([]*FileBlock, 0)
	bs.Remote = make([]int, 0)
	bs.DeviceIds = make([]node.DeviceId, 0)
	bs.Local = make([]int, 0)
	bs.fileBlockMap = make(map[string][]int)
	bs.reqMap = make(map[int]int64)

	i := 0
	for _, file := range tFiles.Files {
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

//过滤掉不可能当前不可能完成的同步 在blockSet 中填充对应的数据
func (sm *SyncManager) filterTargetFiles(tFiles *TargetFiles,
	blockSet *BlockSet,
	reqSet *requestSet) {

	newFiles := make([]*TargetFile, 0)
	for _, file := range tFiles.Files {
		seqs := blockSet.fileBlockMap[file.Name]
		isCompete := true
		for _, seq := range seqs {
			block := blockSet.All[seq]
			if block.From == 0 {
				data, err := sm.getLocalData(block)
				if err != nil {
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
	pretreatedTargetFiles(tFiles)
}

type TargetFolders []*TargetFile

func (ts TargetFolders) Len() int {
	return len(ts)
}

func (ts TargetFolders) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

func (ts TargetFolders) Less(i, j int) bool {
	return len(ts[i].Folder) < len(ts[j].Folder)
}

//对folder 进行排序 移除非法的link文件
func pretreatedTargetFiles(tFiles *TargetFiles) {
	sort.Sort(TargetFolders(tFiles.Folders))
	newLinks := make([]*TargetFile, 0)
	for _, info := range tFiles.Links {
		target := info.Dst.SymlinkTarget
		_, err := filepath.Rel(info.Folder, target)
		if err != nil {
			continue
		}
		newLinks = append(newLinks, info)
	}
	tFiles.Links = newLinks
}

func (sm *SyncManager) getLocalData(block *FileBlock) ([]byte, error) {
	return sm.fsys.GetData(block.Folder, block.Name,
		block.Offset, block.Size)
}

//缺乏对于hash function一致调用方式
func validData(hash, data []byte) bool {
	h := md5.Sum(data)
	if bytes.Compare(hash, h[:]) != 0 {
		return false
	}
	return true
}

func (sm *SyncManager) doSyncFolder(tFolder *TargetFile) *bep.FileInfo {
	var bak *fileBak
	var err error
	filePath, err := sm.GetRealPath(tFolder.Folder, tFolder.Name)
	var needDelete = false
	var needCreate = true
	if err != nil {
		return nil
	}

	permission := os.FileMode(tFolder.Dst.Permissions)
	info, err := os.Stat(filePath)

	if !os.IsNotExist(err) {
		if info.IsDir() {
			log.Printf("%s has exist ", filePath)
			needDelete = true
			needCreate = false
		}

		if hasNewerFile(info, tFolder.Dst) {
			return nil
		}

		if IsLink(info) {
			bak, err = deleteLink(filePath, true)
		} else {
			bak, err = deleteFile(filePath, true)
		}
	}

	if !tFolder.Dst.Deleted {
		if needCreate {
			err = createFolder(filePath, permission)
			if err != nil {
				restoreBak(bak)
				return nil
			}
		}
	} else {
		if needDelete {
			deleteFolderWithOutBak(filePath)
		}
	}

	return tFolder.Dst
}

func IsLink(info os.FileInfo) bool {
	mode := info.Mode()
	if mode.Perm()&os.ModeSymlink > 0 {
		return true
	}
	return false
}

func (sm *SyncManager) doSyncFile(tFile *TargetFile, blockSet *BlockSet) *bep.FileInfo {
	var bak *fileBak
	var err error
	var needDelete = false
	filePath, err := sm.GetRealPath(tFile.Folder, tFile.Name)
	permission := os.FileMode(tFile.Dst.Permissions)
	if err != nil {
		return nil
	}

	info, err := os.Stat(filePath)
	if !os.IsNotExist(err) {
		log.Println("exist ", info.Name())
		if hasNewerFile(info, tFile.Dst) {
			log.Printf("local %s is newer ", tFile.Name)
			return nil
		}

		if info.IsDir() {
			bak, err = deleteFolder(filePath, true)
		} else if IsLink(info) {
			bak, err = deleteLink(filePath, true)
		} else {
			needDelete = true
		}
	}

	if !tFile.Dst.Deleted {
		tmpFile, err := generateTmpFile(tFile, blockSet)
		if err != nil {
			log.Printf("%s generate temp file \n", err.Error())
			goto rollback
		}

		err = createFile(filePath, permission)
		if err != nil {
			log.Printf("%s when create file \n", err.Error())
			goto rollback
		}

		_, err = dupFile(filePath, tmpFile)
		if err != nil {
			log.Printf("%s when dup file \n", err.Error())
			goto rollback
		}
	} else {
		if needDelete {
			_, _ = deleteFile(filePath, false)
		}
	}

	return tFile.Dst

rollback:
	restoreBak(bak)
	return nil
}

func (sm *SyncManager) doSyncLink(tLink *TargetFile) *bep.FileInfo {
	var bak *fileBak
	var err error
	var needDelete = false
	filePath, err := sm.GetRealPath(tLink.Folder, tLink.Name)
	target := tLink.Dst.SymlinkTarget
	info, err := os.Stat(filePath)
	if !os.IsNotExist(err) {
		if hasNewerFile(info, tLink.Dst) {
			return nil
		}

		if info.IsDir() {
			bak, err = deleteFolder(filePath, true)
		} else if IsLink(info) {
			link, _ := os.Readlink(info.Name())
			if link != target {
				bak, err = deleteLink(filePath, true)
				if err != nil {
					return nil
				}
			} else {
				needDelete = true
			}
		} else {
			bak, err = deleteFile(filePath, true)
		}
	}

	if !tLink.Dst.Deleted {
		err = createLink(filePath, target)
		if err != nil {
			goto rollback
		}
	} else {
		if needDelete {
			_, _ = deleteLink(filePath, false)
		}
	}

	return tLink.Dst

rollback:
	restoreBak(bak)
	return nil
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

func logStruct(v interface{}) {
	content, _ := json.MarshalIndent(v, "", " ")
	log.Println(string(content))
}
