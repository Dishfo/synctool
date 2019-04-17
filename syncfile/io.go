package syncfile

import (
	"log"
	"syncfolders/bep"
	"syncfolders/node"
	"time"
)

func (sm *SyncManager) receiveWorker() {
	messages := sm.cn.Messages()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				log.Println("message channel is not available")
				return
			}
			log.Println(msg)
		}
	}
}

func (sm *SyncManager) receiveMsg(msg node.WrappedMessage) {
	realmsg := msg.Msg
	switch realmsg.(type) {
	case *bep.IndexUpdate:
		sm.handleUpdate(msg.Remote, realmsg.(*bep.IndexUpdate))
	case *bep.Index:
		sm.handleIndex(msg.Remote, realmsg.(*bep.Index))
	case *bep.ClusterConfig:
		sm.handleClusterConfig(msg.Remote, realmsg.(*bep.ClusterConfig))
	case *bep.Request:
		sm.handleRequest(msg.Remote, realmsg.(*bep.Request))
	case *bep.Response:
		sm.handleResponse(msg.Remote, realmsg.(*bep.Response))
	}
}

func (sm *SyncManager) handleIndex(remote node.DeviceId,
	index *bep.Index) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("%s when receice index %s %s ",
			err.Error(), index.Folder, remote.String())
	}
	if !HasRelation(tx, index.Folder, remote) {
		return
	}
	sm.temporaryIndex(remote, index)
}

func (sm *SyncManager) handleUpdate(remote node.DeviceId,
	update *bep.IndexUpdate) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("%s when receice index %s %s ",
			err.Error(), update.Folder, remote.String())
	}

	if !HasRelation(tx, update.Folder, remote) {
		return
	}
	sm.temporaryUpdate(remote, update)
}

/**
共享关系中如果新的共享关系中意味一个旧的关系移除
程序本社还要确认是否有共享关系被移除
*/
//todo 锁策略完善
func (sm *SyncManager) handleClusterConfig(remote node.DeviceId,
	config *bep.ClusterConfig) {
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}
	relations := make([]*ShareRelation, 0)
	for _, folder := range config.Folders {
		share := sm.existFolder(folder.Id)
		if share == nil {
			continue
		}
		if !containDevice(remote, share.Devices) {
			sm.folderLock.RUnlock()
			continue
		}
		if !HasRelation(tx, share.Id, remote) {
			relation := &ShareRelation{
				ReadOnly:     share.ReadOnly,
				PeerReadOnly: folder.ReadOnly,
				Folder:       share.Id,
				Remote:       remote,
			}
			_, _ = StoreRelation(tx, relation)
			relations = append(relations, relation)
		}
	}
	devRelations, err := GetRelationOfDevice(tx, remote)
	if err != nil {
		panic(err)
	}
	relationMap := make(map[string]bool)
	for _, r := range devRelations {
		relationMap[r.Folder] = true
	}
	for _, r := range relations {
		if !relationMap[r.Folder] {
			DeleteRelation(tx, r.Id)
		}
	}
	_ = tx.Commit()
}

func (sm *SyncManager) existFolder(folderId string) *ShareFolder {
	sm.folderLock.RLock()
	defer sm.folderLock.RUnlock()
	return sm.folders[folderId]
}

func (sm *SyncManager) handleRequest(remote node.DeviceId,
	req *bep.Request) {
	resp := sm.fsys.GetBlock(req)
	err := sm.SendMessage(remote, resp)
	if err != nil {
		sm.onDisConnected(remote)
	}
}

//handleResponse 暂时缓存resp resp 提交给对应的同步任务
func (sm *SyncManager) handleResponse(remote node.DeviceId,
	resp *bep.Response) {

}

//缓存 index
func (sm *SyncManager) temporaryIndex(remote node.DeviceId,
	index *bep.Index) {
	ri := &ReceiveIndex{
		remote:    remote,
		index:     index,
		timestamp: time.Now().Unix(),
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = StoreReceiveIndex(tx, ri)
	if err != nil {
		log.Printf("%s when cache received index ",
			err.Error())
		_ = tx.Rollback()
		return
	}

	_ = tx.Commit()
}

func (sm *SyncManager) temporaryUpdate(remote node.DeviceId,
	update *bep.IndexUpdate) {
	ri := &ReceiveIndexUpdate{
		remote:    remote,
		update:    update,
		timestamp: time.Now().Unix(),
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = StoreReceiveUpadte(tx, ri)
	if err != nil {
		log.Printf("%s when cache received index ",
			err.Error())
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()

}

func containDevice(remote node.DeviceId, devices []string) bool {
	for _, dev := range devices {
		devId, err := node.GenerateIdFromString(dev)
		if err != nil {
			continue
		}
		if devId == remote {
			return true
		}
	}
	return false
}

func (sm *SyncManager) SendMessage(remote node.DeviceId, msg interface{}) error {
	return sm.cn.SendMessage(remote, msg)
}
