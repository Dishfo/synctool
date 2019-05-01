package syncfile

import (
	"database/sql"
	"errors"
	"github.com/mattn/go-sqlite3"
	"log"
	"reflect"
	"sync"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
)

/**
添加folder 作为参数
*/
type FolderOption struct {
	Id                string
	Label             string
	Real              string
	ReadOnly          bool
	IgnorePermissions bool
	Devices           []string
}

/**
在管理单元中的model
*/
type ShareFolder struct {
	Id                string
	Label             string
	Real              string
	ReadOnly          bool
	IgnorePermissions bool
	Devices           []string
	lock              sync.Mutex
	paused            bool
	isUpdating        bool
	lastUpdate        int64
}

//Copy will deep copy a shareFolder
func (share *ShareFolder) Copy(s *ShareFolder) {
	share.Id = s.Id
	share.ReadOnly = s.ReadOnly
	share.Label = s.Label
	share.IgnorePermissions = s.IgnorePermissions
	share.Real = s.Real
	share.Devices = make([]string, len(s.Devices))
	copy(share.Devices, s.Devices)
}

func (share *ShareFolder) Attribute() *FolderAttribute {
	attr := new(FolderAttribute)
	attr.Id = share.Id
	attr.IgnorePermissions = share.IgnorePermissions
	attr.Label = share.Label
	attr.ReadOnly = share.ReadOnly
	attr.Real = share.Real
	attr.Devices = make([]string, len(share.Devices))
	copy(attr.Devices, share.Devices)
	return attr
}

/**
shareFolder 的一个视图
*/
type FolderAttribute struct {
	Id                string
	Label             string
	Real              string
	ReadOnly          bool
	IgnorePermissions bool
	Devices           []string
	Paused            bool
	IsUpdating        bool
}

var (
	ErrInvalidFolderId = errors.New("the id of folder is invalid ")
	ErrRepeatFolderId  = errors.New("exist a same folder id ")
	ErrInvalidOpts     = errors.New("options is not invalid")
	ErrNonsexist       = errors.New("folder doesn't exist")
)

//todo 对于其他设备需要确认 共享关系是否发生变化
//todo 对于本地同样需要关注共享关系是否变化,此时需要获取到已有的config进行计算
func (sm *SyncManager) AddFolder(opt *FolderOption) error {
	if opt == nil || opt.Id == "" {
		return ErrInvalidFolderId
	}

	err := sm.fsys.AddFolder(opt.Id, opt.Real)
	if err != nil {
		return err
	}

	sm.folderLock.Lock()

	if _, ok := sm.folders[opt.Id]; ok {
		return ErrRepeatFolderId
	}

	f := newShareFolder(opt)
	sm.folders[opt.Id] = f
	sm.onAddFolder(f)
	sm.folderLock.Unlock()
	sm.onFoldersChange()
	return nil
}

func (sm *SyncManager) GetFolders() []FolderAttribute {
	sm.folderLock.RLock()
	defer sm.folderLock.RUnlock()
	attrs := make([]FolderAttribute, 0)
	for _, f := range sm.folders {
		var attr FolderAttribute
		attr = *f.Attribute()
		attrs = append(attrs, attr)
	}
	return attrs
}

func newShareFolder(opt *FolderOption) *ShareFolder {
	share := new(ShareFolder)
	share.Id = opt.Id
	share.Real = opt.Real
	share.Label = opt.Label
	share.ReadOnly = opt.ReadOnly
	share.IgnorePermissions = opt.IgnorePermissions
	share.Devices = make([]string, len(opt.Devices))
	for i, d := range opt.Devices {
		share.Devices[i] = d
	}
	return share
}

//修改folder属性
func (sm *SyncManager) EditFolder(opts map[string]interface{},
	folderId string) error {
	sm.folderLock.Lock()

	if id, ok := opts["Id"]; ok {
		if id.(string) != folderId {
			return errors.New("shouldn't set new folderId")
		}
	}

	if share, ok := sm.folders[folderId]; ok {
		newShare := new(ShareFolder)
		newShare.Copy(share)
		for k, v := range opts {
			rv := reflect.ValueOf(newShare)
			field := rv.FieldByName(k)
			if !field.IsValid() {
				sm.folderLock.Unlock()
				return ErrInvalidOpts
			}
			err := setValue(field, v)
			if err != nil {
				sm.folderLock.Unlock()
				return err
			}
		}
		sm.folders[folderId] = newShare
		sm.onEditFolder(newShare, share.Attribute())
		sm.folderLock.Unlock()
		sm.onFoldersChange()
	} else {
		sm.folderLock.Unlock()
		return ErrNonsexist
	}
	return nil
}

func (sm *SyncManager) RemoveFolder(folderId string) {
	sm.folderLock.Lock()
	defer sm.folderLock.Unlock()
	sm.fsys.RemoveFolder(folderId)
	sm.onFoldersChange()
}

var (
	ErrNotMatchType = errors.New("field is not macth with value ")
	ErrCantSetField = errors.New("can't set this field ")
)

func setValue(field reflect.Value, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != field.Kind() {
		return ErrNotMatchType
	}
	if !field.CanSet() {
		return ErrCantSetField
	}
	field.Set(rv)
	return nil

}

//在	添加节点的共享构成发生变化时使用 向各个节点发送新的 clusterConfig
func (sm *SyncManager) onFoldersChange() {
	devIds := sm.getConnectedDevice()
	config := sm.generateClusterConfig()
	for _, dev := range devIds {
		err := sm.SendMessage(dev, config)
		if err != nil {
			sm.cn.DisConnect(dev)
			sm.onDisConnected(dev)
		}
	}
}

//todo 修改此处的程序逻辑
//callback when　edit a folder 修改一个folder属性后　可能
//会移除共享关系　也可能会产生新的共享关系
func (sm *SyncManager) onEditFolder(folder *ShareFolder, older *FolderAttribute) {
	toDel := make([]string, 0)
	toAdd := make([]string, 0)

	oldDevMap := make(map[string]bool)
	newDevMap := make(map[string]bool)

	for _, s := range older.Devices {
		oldDevMap[s] = true
	}

	for _, s := range folder.Devices {
		newDevMap[s] = true
		if !oldDevMap[s] {
			toAdd = append(toAdd, s)
		}
	}

	for _, s := range older.Devices {
		if !newDevMap[s] {
			toDel = append(toDel, s)
		}
	}

	for _, dev := range toDel {
		devId, err := node.GenerateIdFromString(dev)
		if err != nil {
			continue
		}
		sm.DeleteRelations(older.Id, devId)
	}

	for _, dev := range toAdd {
		devId, err := node.GenerateIdFromString(dev)
		if err != nil {
			continue
		}

		folder := getFolder(older.Id, devId)
		if folder == nil {
			continue
		}

		sm.UpdateRelation(&ShareRelation{
			Folder:       older.Id,
			Remote:       devId,
			ReadOnly:     folder.ReadOnly,
			PeerReadOnly: folder.ReadOnly,
		})
	}

}

//callback when add a folder
func (sm *SyncManager) onAddFolder(folder *ShareFolder) {
	localId := sm.LocalId()
	devices := sm.Devices()
	for _, dev := range devices {
		devId := node.GenerateIdFromBytes(dev.Id)
		img := getFoldersImages(devId)
		if img == nil {
			continue
		}

		for _, f := range img.folders {
			if f.Id == folder.Id {
				r := calculateRelations(folder, f, localId)
				if r == nil {
					continue
				}
				r.Remote = devId
				tx, err := sm.cacheDb.Begin()
				if err != nil {
					panic(err)
				}
				_, err = storeRelation(tx, r)
				if err != nil {
					log.Printf("%s when  store relations ",
						err.Error())
					_ = tx.Rollback()
				} else {
					_ = tx.Commit()
				}
			}
		}
	}
}

//used by onAddFolder
func calculateRelations(sf *ShareFolder,
	f *bep.Folder, localId node.DeviceId) *ShareRelation {
	var relation *ShareRelation = nil
	for _, dev := range f.Devices {
		devId := node.GenerateIdFromBytes(dev.Id)
		if devId == localId {
			relation = new(ShareRelation)
			relation.Folder = sf.Id
			relation.ReadOnly = sf.ReadOnly
			relation.PeerReadOnly = f.ReadOnly
			return relation
		}
	}
	return relation
}

//修正标记位表示sync模块开始进行
func (sm *SyncManager) StartSendUpdate() bool {
	sm.folderLock.Lock()
	defer sm.folderLock.Unlock()
	if sm.inSendUpdateTranscation {
		return false
	}
	sm.inSendUpdateTranscation = true
	return true
}

//设置对应标记
func (sm *SyncManager) EndSendUpdate() {
	sm.folderLock.Lock()
	defer sm.folderLock.Unlock()
	sm.inSendUpdateTranscation = false
}

//使用重试 ?????? 添加针对 database locked 测试阶段先不要添加
//todo 我还要靠这个找bug ^ __ ^
//定期执行的任务 修改逻辑避事务中穿插过多的 i/o

/**
sendUpdate 是单线程的行为,sendUpdate仅仅只是读写sendUpdate表
并且读取 received update ,在一个时刻如果没有读取到就主观认为没有收到

*/
func (sm *SyncManager) prepareSendUpdate() {
	var err error
	var tx *sql.Tx
	if !sm.StartSendUpdate() {
		return
	}

	defer sm.EndSendUpdate()

	devIds := sm.getConnectedDevice()

	for _, dev := range devIds {
		tx, err = sm.cacheDb.Begin()
		if err != nil {
			log.Printf("%s when preparet to send updates ",
				err.Error())
			return
		}
		relations, err := getRelationOfDevice(tx, dev)
		if err != nil {
			_ = tx.Rollback()
			log.Printf(" %s when get relations of %s ",
				err.Error(), dev.String())
			_ = tx.Rollback()
			return
		}
		_ = tx.Commit()
		for _, relation := range relations {
			if relation.PeerReadOnly {
				continue
			}
			tx, err = sm.cacheDb.Begin()
			if err != nil {
				log.Printf("%s when preparet to send updates ",
					err.Error())
				return
			}

			sus, err := getSendUpdateToFolder(tx, dev, relation.Folder)
			if err != nil {
				log.Printf(" %s when get SendUpdates of %s ",
					err.Error(), dev.String())
				_ = tx.Rollback()
				return
			}
			_ = tx.Commit()

			tagMap := make(map[int64]bool)
			for _, su := range sus {
				tagMap[su.UpdateId] = true
			}

			indexSeqs := sm.fsys.GetIndexSeqAfter(relation.Folder, 0)
			if err != nil {
				log.Printf(" %s whecd n get indexSeqs of %s ",
					err.Error(), dev.String())
				return
			}

			readySend := make([]*fs.IndexSeq, 0)
			for _, indexSeq := range indexSeqs {
				if !tagMap[indexSeq.Id] {
					readySend = append(readySend, indexSeq)
				}
			}

			indexs, updates :=
				sm.getUpdatesByIndexSeq(relation.Folder, readySend)
			for id, index := range indexs {
				log.Println(index)
				sm.sendUpdate(dev,
					index, relation.Folder, id)
			}

			logStruct(updates)
			for id, update := range updates {
				sm.sendUpdate(dev,
					update, relation.Folder, id)
			}
		}
	}
}

func (sm *SyncManager) sendUpdate(remote node.DeviceId,
	data interface{}, folderId string, uid int64) {
	err := sm.SendMessage(remote, data)
	if err == nil {
		su := &SendUpdate{
			Folder:   folderId,
			UpdateId: uid,
			Remote:   remote,
		}
		sm.storeSendUpdate(su)
	} else {
		log.Println(err.Error())
		sm.DisConnection(remote)
	}
}

//严格区分ind	ex indexUpdate
func (sm *SyncManager) getUpdatesByIndexSeq(folderId string, indexSeqs []*fs.IndexSeq) (map[int64]*bep.Index,
	map[int64]*bep.IndexUpdate) {
	return sm.fsys.GetIndexUpdateMap(folderId, indexSeqs)
}

//relation operation
//保证删除成功
func (sm *SyncManager) DeleteRelations(folderId string, remote node.DeviceId) {
	err := sm.deleteRelation(folderId, remote)
	for err != sqlite3.ErrLocked {
		err = sm.deleteRelation(folderId, remote)
	}
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func (sm *SyncManager) UpdateRelation(relation *ShareRelation) {
	err := sm.updateRelation(relation)
	for err != sqlite3.ErrLocked {
		err = sm.updateRelation(relation)
	}
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func (sm *SyncManager) updateRelation(relation *ShareRelation) error {
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		return err
	}

	if !hasRelation(tx, relation.Folder, relation.Remote) {
		_, err = storeRelation(tx, relation)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	} else {
		err = updateRelation(tx, relation)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	_ = tx.Commit()
	return nil
}

func (sm *SyncManager) deleteRelation(folderId string, remote node.DeviceId) error {
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()
	return DeleteRelationSpec(tx, folderId, remote)
}
