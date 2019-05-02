package syncfile

import (
	"bytes"
	"crypto/md5"
	"github.com/mattn/go-sqlite3"
	"log"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
	"time"
)

//todo 事务的隔离等级是程序逻辑能否顺利执行的关键
//接收函数也是单线程 舒服啊
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
		log.Println("receive config ", realmsg.(*bep.ClusterConfig))
		onReceiveClusterConfig(msg.Remote, realmsg.(*bep.ClusterConfig))
		sm.handleClusterConfig(msg.Remote, realmsg.(*bep.ClusterConfig))
	case *bep.Request:
		sm.handleRequest(msg.Remote, realmsg.(*bep.Request))
	case *bep.Response:
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

	if !hasRelation(tx, index.Folder, remote) {
		tx.Commit()
		return
	}

	tx.Commit()
	err = sm.temporaryIndex(remote, index)
	for err != nil {
		log.Printf("repeat for %s \n", err.Error())
		err = sm.temporaryIndex(remote, index)
	}
}

func (sm *SyncManager) handleUpdate(remote node.DeviceId,
	update *bep.IndexUpdate) {
	tx, err := sm.cacheDb.Begin()

	if err != nil {
		log.Fatalf("%s when receice index %s %s ",
			err.Error(), update.Folder, remote.String())
	}

	if !hasRelation(tx, update.Folder, remote) {
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

/**
节点可能会发送多个config,有可能出现某个关系的移除
获取原来的关系表，
构建新的关系表
保证这个处理过程为线程安全
*/

//todo 此处的事务需要是串行
func (sm *SyncManager) handleClusterConfig(remote node.DeviceId,
	config *bep.ClusterConfig) {
	//log.Printf("%s receied from %s ", config.String(),
	//	remote.String())

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
		relation := &ShareRelation{
			ReadOnly:     share.ReadOnly,
			PeerReadOnly: folder.ReadOnly,
			Folder:       share.Id,
			Remote:       remote,
		}
		relations = append(relations, relation)
	}
	oldRelations, err := sm.GetRelationOfDevice(remote)
	if err != nil {
		panic(err)
	}
	log.Println("calculate relations ", relations)

	_ = tx.Commit()
	sm.setNewRelation(oldRelations, relations)
}

//需要保证后面的数据库操作成功，不然就终止整个程序
func (sm *SyncManager) setNewRelation(oldRelations, relations []*ShareRelation) {
	toDelete := make([]int64, 0)
	toAdd := make([]*ShareRelation, 0)

	relationMap := make(map[string]*ShareRelation)
	newRelationMap := make(map[string]*ShareRelation)
	for _, r := range oldRelations {
		relationMap[r.Folder] = r
	}

	for _, r := range relations {
		newRelationMap[r.Folder] = r
		if relationMap[r.Folder] == nil {
			toAdd = append(toAdd, r)
		}
	}

	for _, r := range oldRelations {
		if newRelationMap[r.Folder] == nil {
			toDelete = append(toDelete, r.Id)
		} else {
			toAdd = append(toAdd, newRelationMap[r.Folder])
		}
	}

	err := sm.addRelations(toAdd)
	for err == sqlite3.ErrLocked {
		log.Println(err, " when insert relations")
		err = sm.addRelations(toAdd)
	}

	err = sm.deleteRelations(toDelete)
	for err == sqlite3.ErrLocked {
		log.Println(err, " when delete relations")
		err = sm.deleteRelations(toDelete)
	}

	if err != nil {
		log.Panic(err)
	}
}

func (sm *SyncManager) deleteRelations(ids []int64) error {
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		return err
	}

	for _, id := range ids {
		err := DeleteRelation(tx, id)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

//内部调用的是update 操作防止出现重复的relations
func (sm *SyncManager) addRelations(relations []*ShareRelation) error {
	var err error
	for _, r := range relations {
		log.Println("will insert relation ", r)
		err = sm.updateRelation(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sm *SyncManager) GetRelationOfDevice(device node.DeviceId) ([]*ShareRelation, error) {
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()
	return getRelationOfDevice(tx, device)
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
	resp.Id = req.Id
	if err != nil {
		log.Printf("%s when response ", err.Error())
		switch err {
		case fs.ErrInvalidSize:
			resp.Code = bep.ErrorCode_GENERIC
		case fs.ErrInvalidFile:
			resp.Code = bep.ErrorCode_INVALID_FILE
		case fs.ErrNoSuchFile:
			resp.Code = bep.ErrorCode_NO_SUCH_FILE
		}
	} else {
		if block == nil {
			resp.Code = bep.ErrorCode_GENERIC
			goto send
		}
		hash := md5.Sum(block)
		if bytes.Compare(hash[:], req.Hash) != 0 {
			resp.Code = bep.ErrorCode_GENERIC
			goto send
		}
		resp.Data = block
	}

send:
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
