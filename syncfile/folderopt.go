package syncfile

import (
	"database/sql"
	"errors"
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
	share.Devices = make([]string, len(s.Devices), len(s.Devices))

	copy(share.Devices, s.Devices)
}

func (share *ShareFolder) Attribute() *FolderAttribute {
	attr := new(FolderAttribute)
	attr.Id = share.Id
	attr.IgnorePermissions = share.IgnorePermissions
	attr.Label = share.Label
	attr.ReadOnly = share.ReadOnly
	attr.Real = share.Real
	attr.Devices = make([]string, len(share.Devices), len(share.Devices))
	copy(attr.Devices, share.Devices)

	return attr
}

/**
shareFolde 的一个视图
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

	sm.folders[opt.Id] = newShareFolder(opt)
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

//修改 folder 属性
func (sm *SyncManager) EditFolder(opts map[string]interface{},
	folderId string) error {
	sm.folderLock.Lock()

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
	} else {
		sm.folderLock.Unlock()
		return ErrNonsexist
	}
	sm.folderLock.Unlock()
	sm.onFoldersChange()
	return nil
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
	for _,dev := range devIds {
		err:=sm.SendMessage(dev,config)
		if err!=nil {
			sm.onDisConnected(dev)
		}
	}
}

//修正标记位  表示sync 模块开始进行
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

//定期执行的任务
func (sm *SyncManager) prepareSendUpdate() {
	var err error
	var otx *sql.Tx
	var tx *sql.Tx
	if !sm.StartSendUpdate() {
		return
	}
	defer sm.EndSendUpdate()

	devIds := sm.getConnectedDevice()
	otx, err = fs.GetTx()
	if err != nil {
		panic(err)
	}

	tx, err = db.Begin()
	if err != nil {
		panic(err)
	}

	for _, dev := range devIds {
		relations, err := GetRelationOfDevice(tx, dev)
		if err != nil {
			log.Printf(" %s when get relations of %s ",
				err.Error(), dev.String())
			goto end
		}
		for _, relation := range relations {
			if relation.PeerReadOnly {
				continue
			}
			sus, err := GetSendUpdateToFolder(tx, dev, relation.Folder)
			if err != nil {
				log.Printf(" %s when get SendUpdates of %s ",
					err.Error(), dev.String())
				goto end
			}
			tagMap := make(map[int64]bool)
			for _, su := range sus {
				tagMap[su.UpdateId] = true
			}
			indexSeqs, err := fs.GetIndexSeqAfter(otx, 0, relation.Folder)
			if err != nil {
				log.Printf(" %s when get indexSeqs of %s ",
					err.Error(), dev.String())
				goto end
			}
			readySend := make([]*fs.IndexSeq, 0)
			for _, indexSeq := range indexSeqs {
				if !tagMap[indexSeq.Id] {
					readySend = append(readySend, indexSeq)
				}
			}
			indexs, updates := sm.getUpdatesByIndexSeq(readySend)
			for id, index := range indexs {
				sm.sendUpdate(dev,
					index,relation.Folder,tx,id)
			}
			for id, update := range updates {
				sm.sendUpdate(dev,
					update,relation.Folder,tx,id)
			}
		}
	}
end :
	if err== nil {
		_ = otx.Commit()
		_ = tx.Commit()
	}else {
		_ = otx.Rollback()
		_ = tx.Rollback()
	}
}

func  (sm *SyncManager) sendUpdate(remote node.DeviceId,
	data interface{},folderId string,tx *sql.Tx,uid int64){
	err := sm.SendMessage(remote, data)
	if err == nil {
		su := &SendUpdate{
			Folder:   folderId,
			UpdateId: uid,
			Remote:   remote,
		}
		_, _ = StoreSendUpdate(tx, su)
	}
}

//严格区分index indexUpdate
func (sm *SyncManager) getUpdatesByIndexSeq(indexSeqs []*fs.IndexSeq) (map[int64]*bep.Index,
	map[int64]*bep.IndexUpdate) {
	return  sm.fsys.GetIndexUpdates(indexSeqs)
}
