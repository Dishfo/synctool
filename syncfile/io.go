package syncfile

import (
	"log"
	"syncfolders/bep"
	"syncfolders/node"
	"time"
)

//todo 事务的隔离等级是程序逻辑能否顺利执行的关键
func (sm *SyncManager) receiveWorker() {
	messages := sm.cn.Messages()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				log.Println("message channel is not available")
				return
			}
			sm.receiveMsg(msg)
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
		onReceiveClusterConfig(msg.Remote, realmsg.(*bep.ClusterConfig))
		sm.handleClusterConfig(msg.Remote, realmsg.(*bep.ClusterConfig))
	case *bep.Request:
		sm.handleRequest(msg.Remote, realmsg.(*bep.Request))
	case *bep.Response:
		log.Printf("receive resp ")
		logStruct(realmsg.(*bep.Response))
		sm.handleResponse(msg.Remote, realmsg.(*bep.Response))
	}
}

func (sm *SyncManager) handleIndex(remote node.DeviceId,
	index *bep.Index) {
	tx, err := sm.cacheDb.Begin()

	if err != nil {
		log.Fatalf("%s when receice index %s %s ",
			err.Error(), index.Folder, remote.String())
	}

	if !HasRelation(tx, index.Folder, remote) {
		tx.Commit()
		return
	}

	tx.Commit()
	err = sm.temporaryIndex(remote, index)
	for err != nil {
		log.Printf("repeat for %s \n", err.Error())
		err = sm.temporaryIndex(remote, index)
	}
	log.Println("compete")
}

func (sm *SyncManager) handleUpdate(remote node.DeviceId,
	update *bep.IndexUpdate) {
	tx, err := sm.cacheDb.Begin()

	if err != nil {
		log.Fatalf("%s when receice index %s %s ",
			err.Error(), update.Folder, remote.String())
	}

	if !HasRelation(tx, update.Folder, remote) {
		tx.Commit()
		return
	}
	tx.Commit()

	err = sm.temporaryUpdate(remote, update)
	for err != nil {
		log.Printf("repeat for %s \n", err.Error())
		err = sm.temporaryUpdate(remote, update)
	}

	log.Println("compete")
}

//先确立连接后 在发送 config  保证任何一方不会丢失数据
func (sm *SyncManager) handleClusterConfig(remote node.DeviceId,
	config *bep.ClusterConfig) {
	log.Printf("%s receied from %s ", config.String(),
		remote.String())
	tx, err := sm.cacheDb.Begin()
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
	resp := new(bep.Response)
	block, err := sm.fsys.GetData(req.Folder,
		req.Name, req.Offset, req.Size)

	if err != nil {
		log.Printf("%s when response ", err.Error())
		resp.Code = bep.ErrorCode_GENERIC
	} else {
		resp.Data = block
	}
	resp.Id = req.Id

	err = sm.SendMessage(remote, resp)
	if err != nil {
		sm.onDisConnected(remote)
	}
}

//handleResponse 暂时缓存resp resp 提交给对应的同步任务
func (sm *SyncManager) handleResponse(remote node.DeviceId,
	resp *bep.Response) {
	sm.rwm.Present(resp)
}

//缓存 index
func (sm *SyncManager) temporaryIndex(remote node.DeviceId,
	index *bep.Index) error {
	ri := &ReceiveIndex{
		remote:    remote,
		index:     index,
		timestamp: time.Now().Unix(),
	}
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		panic(err)
	}
	log.Println("remoteID ", remote)
	_, err = storeReceiveIndex(tx, ri)
	if err != nil {
		log.Printf("%s when cache received index ",
			err.Error())
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	return err
}

func (sm *SyncManager) temporaryUpdate(remote node.DeviceId,
	update *bep.IndexUpdate) error {
	ri := &ReceiveIndexUpdate{
		remote:    remote,
		update:    update,
		timestamp: time.Now().Unix(),
	}
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		panic(err)
	}
	log.Println("remoteID ", remote)
	_, err = storeReceiveUpadte(tx, ri)
	if err != nil {
		log.Printf("%s when cache received index ",
			err.Error())
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
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
