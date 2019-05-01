package syncfile

import (
	"database/sql"
	"log"
	"sync"
	"syncfolders/bep"
	"syncfolders/node"
)

/**
提供对于model的定义
*/

/**
描述设备间的共享关系
这个	，模型使用内存来进行存储
*/

//todo 在程序中使用单例 保证不会有多个 fs 或 syncManager
type ShareRelation struct {
	Id           int64
	Folder       string
	ReadOnly     bool
	PeerReadOnly bool
	Remote       node.DeviceId
}

type ShareFoldersImage struct {
	remote  node.DeviceId
	folders []*bep.Folder
}

var (
	folderImages = make(map[node.DeviceId]*ShareFoldersImage)
	lock         sync.Mutex
)

//记录收到的cluster Config中的folders
func onReceiveClusterConfig(remote node.DeviceId,
	config *bep.ClusterConfig) {
	image := new(ShareFoldersImage)
	image.remote = remote
	image.folders = config.Folders
	lock.Lock()
	defer lock.Unlock()
	folderImages[remote] = image
}

func getFoldersImages(remote node.DeviceId) *ShareFoldersImage {
	lock.Lock()
	defer lock.Unlock()
	return folderImages[remote]
}

func getFolder(folderId string, remote node.DeviceId) *bep.Folder {
	lock.Lock()
	defer lock.Unlock()
	img := folderImages[remote]
	if img == nil {
		return nil
	}
	for _, folder := range img.folders {
		if folder.Id == folderId {
			return folder
		}
	}

	return nil
}

/**
提供关系存储的访问函数
*/

func DeleteRelation(tx *sql.Tx, id int64) error {
	stmt, _ := tx.Prepare(deleteRelation)
	_, err := stmt.Exec(id)

	return err
}

func DeleteRelationSpec(tx *sql.Tx, folder string, remote node.DeviceId) error {
	stmt, _ := tx.Prepare(deleteRelationByRemoteAndFolder)

	_, err := stmt.Exec(folder, int64(remote))

	return err
}

func storeRelation(tx *sql.Tx, r *ShareRelation) (int64, error) {
	stmt, err := tx.Prepare(insertRelation)
	if err != nil {
		return 0, err
	}

	res, err := stmt.Exec(r.Folder,
		r.ReadOnly,
		r.PeerReadOnly,
		int64(r.Remote))

	if err != nil {
		return 0, err
	}

	id, _ := res.LastInsertId()
	return id, nil
}

func updateRelation(tx *sql.Tx, r *ShareRelation) error {
	stmt, err := tx.Prepare(updateRelationsql)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(r.PeerReadOnly, r.ReadOnly,
		int64(r.Remote), r.Folder)
	if err != nil {
		return err
	}
}

func getRelationOfDevice(tx *sql.Tx,
	remote node.DeviceId) ([]*ShareRelation, error) {
	return interalSelectRelation(tx, selectRelationOfDevice, int64(remote))
}

func interalSelectRelation(tx *sql.Tx, sqlcmd string, values ...interface{}) ([]*ShareRelation, error) {
	stmt, err := tx.Prepare(sqlcmd)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	relations := make([]*ShareRelation, 0)
	for rows.Next() {
		relation := new(ShareRelation)
		err := fillRelation(rows, relation)
		if err != nil {
			return nil, err
		}
		relations = append(relations, relation)
	}
	return relations, nil
}

func hasRelation(tx *sql.Tx, folder string, remote node.DeviceId) bool {
	stmt, err := tx.Prepare(selectRelationFolderWithDev)
	if err != nil {
		return false
	}
	rows, err := stmt.Query(folder, int64(remote))
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func fillRelation(rows *sql.Rows, relation *ShareRelation) error {
	var remote int64
	_ = rows.Scan(&relation.Id,
		&relation.Folder,
		&relation.ReadOnly,
		&relation.PeerReadOnly,
		&remote)
	relation.Remote = node.DeviceId(remote)
	return nil
}
