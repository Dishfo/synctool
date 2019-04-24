package syncfile

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"strconv"
	"strings"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
)

/**
in-memory sql
*/

/**
todo 事务处理中应该有更加细致的隔离等级划分
*/
var (
	db *sql.DB
)

const (
	typeIndex  = 1
	typeUpdate = 2

	createRelationTable = `
	create table ShareRelation (
	id integer not null primary key autoincrement,
	folder text,
	readonly integer,
	peerreadonly integer,
	remote  integer
	)	
	`

	createReceiveTable = `
	create table ReceiveUpdate(
	id integer not null primary key autoincrement,
	folder text,
	remote integer,
	type integer,
	seqs text,
	timestamp integer
	)
	`

	createSendUpdate = `
	create table SendUpdate (
	id integer not null primary key autoincrement,
	folder text,
	remote integer,
	updateId integer
	)
	`

	dbFile = "core.db"
)

func init() {
	var err error
	_ = os.Remove(dbFile)
	db, err = sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatalf("%s when init in-memory databases ",
			err.Error())
	}

	_, err = db.Exec(createRelationTable)
	if err != nil {
		log.Fatalf("%s when create relation table",
			err.Error())
	}

	_, err = db.Exec(createReceiveTable)
	if err != nil {
		log.Fatalf("%s when create receive table",
			err.Error())
	}

	_, err = db.Exec(createSendUpdate)
	if err != nil {
		log.Fatalf("%s when create sendUpdate table",
			err.Error())
	}
}

const (
	insertRelation = `
	insert into ShareRelation 
	(folder,readonly,peerreadonly,remote)
	values (?,?,?,?)
	`

	selectRelationOfFolder = `
	select 
	id,folder,readonly,peerreadonly,remote  
	from ShareRelation 
	where folder = ?
	`

	selectRelationOfDevice = `
	select 
	id,folder,readonly,peerreadonly,remote 
	from ShareRelation 
	where remote = ?
	`

	selectRelationFolderWithDev = `
	select 
	id,folder,readonly,peerreadonly,remote  
	from ShareRelation 
	where folder = ? and remote = ?
	`

	deleteRelation = `
	delete from ShareRelation
	where id = ?
	`

	deleteRelationByRemoteAndFolder = `
	delete from ShareRelaton 
	where folder = ? and remote = ? 
	`
)

const (
	insertReceiveIndex = `
	insert into ReceiveUpdate 
	(folder ,remote,type,seqs,timestamp) 
	values (?,?,?,?,?)
	`

	selectReceiveUpdate = `
	select	id, folder ,remote,seqs,timestamp 
	from ReceiveUpdate 
	where id = ? 
	`

	selectReceiveUpdateAfter = `
	select	id, folder ,remote,seqs,timestamp 
	from ReceiveUpdate 
	where id > ? and folder = ?
	`
)

/**
提供receiveUpdate 访问函数
*/

func StoreReceiveUpadte(tx *sql.Tx, update *ReceiveIndexUpdate) (int64, error) {
	return storeReceivedData(tx, update)
}

func StoreReceiveIndex(tx *sql.Tx, index *ReceiveIndex) (int64, error) {
	return storeReceivedData(tx, index)
}

func storeReceivedData(tx *sql.Tx, data interface{}) (int64, error) {
	var folder string
	var remote node.DeviceId
	var rtype int
	var timestamp int64
	seqs := make([]int64, 0)
	infos := make([]*bep.FileInfo, 0)

	switch data.(type) {
	case *ReceiveIndex:
		receindex := data.(*ReceiveIndex)
		remote = receindex.remote
		infos = receindex.index.Files
		folder = receindex.index.Folder
		rtype = typeIndex
		timestamp = receindex.timestamp
	case *ReceiveIndexUpdate:
		receupdate := data.(*ReceiveIndexUpdate)
		remote = receupdate.remote
		infos = receupdate.update.Files
		folder = receupdate.update.Folder
		rtype = typeUpdate
		timestamp = receupdate.timestamp
	}

	otx, err := fs.GetTx()
	if err != nil {
		return 0, fmt.Errorf("%s when insert fileinfo ", err.Error())
	}

	for _, info := range infos {
		id, err := fs.StoreFileinfo(otx, folder, info)
		if err != nil {
			_ = otx.Rollback()
			return 0, err
		}
		seqs = append(seqs, id)
	}
	_ = otx.Commit()

	seqsstr := ArrayToString(seqs)
	stmt, err := tx.Prepare(insertReceiveIndex)
	res, err := stmt.Exec(folder,
		int64(remote),
		rtype,
		seqsstr,
		timestamp)
	if err != nil {
		panic(err)
	}
	id, _ := res.RowsAffected()
	return id, nil
}

func GetReceiveIndex(tx *sql.Tx, id int64) ([]*ReceiveIndex, error) {
	receiveUpdates, err := GetReceiveUpdate(tx, id)
	if err != nil {
		return nil, err
	}

	receiveIndexs := make([]*ReceiveIndex, 0)

	for _, u := range receiveUpdates {
		i := new(ReceiveIndex)
		i.timestamp = u.timestamp
		i.remote = u.remote
		i.index = new(bep.Index)
		i.index.Folder = u.update.Folder
		i.index.Files = u.update.Files
		receiveIndexs = append(receiveIndexs, i)
	}

	return receiveIndexs, nil
}

func GetReceiveUpdate(tx *sql.Tx, id int64) ([]*ReceiveIndexUpdate, error) {
	stmt, err := tx.Prepare(selectReceiveUpdate)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	updates := make([]*ReceiveIndexUpdate, 0)
	for rows.Next() {
		update := new(ReceiveIndexUpdate)
		var folder string
		var remote int64
		var seqs string
		var timestamp int64
		err := rows.Scan(&update.Id,
			&folder,
			&remote,
			&seqs,
			&timestamp)
		if err != nil {
			return nil, err
		}
		otx, err := fs.GetTx()
		if err != nil {
			return nil, err
		}

		ids := StringToArray(seqs)
		infos, err := fs.GetFileInfos(otx, ids)
		if err != nil {
			_ = otx.Rollback()
			return nil, err
		}

		update.timestamp = timestamp
		update.remote = node.DeviceId(remote)
		update.update = new(bep.IndexUpdate)
		update.update.Folder = folder
		update.update.Files = infos

		updates = append(updates, update)
		_ = otx.Commit()
	}

	return updates, nil
}

func GetReceiveUpdateAfter(tx *sql.Tx, id int64, folderId string) ([]*ReceiveIndexUpdate, error) {
	stmt, err := tx.Prepare(selectReceiveUpdateAfter)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(id, folderId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	updates := make([]*ReceiveIndexUpdate, 0)
	for rows.Next() {
		update := new(ReceiveIndexUpdate)
		var folder string
		var remote int64
		var seqs string
		var timestamp int64
		err := rows.Scan(&update.Id,
			&folder,
			&remote,
			&seqs,
			&timestamp)
		if err != nil {
			return nil, err
		}
		otx, err := fs.GetTx()
		if err != nil {
			return nil, err
		}

		ids := StringToArray(seqs)
		infos, err := fs.GetFileInfos(otx, ids)
		if err != nil {
			_ = otx.Rollback()
			return nil, err
		}

		_ = otx.Commit()
		update.timestamp = timestamp
		update.remote = node.DeviceId(remote)
		update.update = new(bep.IndexUpdate)
		update.update.Folder = folder
		update.update.Files = infos
		updates = append(updates, update)
	}

	return updates, nil
}

func ArrayToString(a []int64) string {
	builder := new(strings.Builder)
	sep := ""
	for _, n := range a {
		builder.WriteString(sep)
		builder.WriteString(strconv.FormatInt(n, 10))
		sep = ","
	}

	return builder.String()
}

func StringToArray(s string) []int64 {
	strs := strings.Split(s, ",")
	res := make([]int64, 0)
	for _, str := range strs {
		n, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			continue
		}
		res = append(res, n)
	}
	return res
}

/**
发送记录 以此为依据获取 到没有发送的 update
定期发送出去
*/
//todo

var (
	insertSendUpdate = `
	insert into SendUpdate 
	(folder,remote,updateId) 
	values (?,?,?) `

	selectSendUpdateByDevice = `
	select id,folder,remote,updateId from 
	SendUpdate where 
	remote = ? 
	`

	selectSendUpdateByDevAndFolder = `
	select id,folder,remote,updateId from  
	SendUpdate where   
	remote = ? and folder = ? 		
	`
)

type SendUpdate struct {
	Id       int64
	UpdateId int64
	Folder   string
	Remote   node.DeviceId
}

func StoreSendUpdate(tx *sql.Tx, su *SendUpdate) (int64, error) {
	stmt, err := tx.Prepare(insertSendUpdate)
	if err != nil {
		return 0, err
	}

	res, err := stmt.Exec(su.Folder,
		int64(su.Remote),
		su.UpdateId)
	if err != nil {
		return 0, err
	}

	id, _ := res.LastInsertId()
	return id, nil
}

func GetSendUpdateOfDevice(tx *sql.Tx, remote node.DeviceId) ([]*SendUpdate, error) {
	stmt, err := tx.Prepare(selectSendUpdateByDevice)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(int64(remote))

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	sus := make([]*SendUpdate, 0)
	for rows.Next() {
		su := new(SendUpdate)
		fillSendUpdate(rows, su)
		sus = append(sus, su)
	}

	return sus, nil
}

func GetSendUpdateToFolder(tx *sql.Tx,
	remote node.DeviceId, folder string) ([]*SendUpdate, error) {
	stmt, err := tx.Prepare(selectSendUpdateByDevAndFolder)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(int64(remote), folder)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	sus := make([]*SendUpdate, 0)
	for rows.Next() {
		su := new(SendUpdate)
		fillSendUpdate(rows, su)
		sus = append(sus, su)
	}

	return sus, nil
}

func fillSendUpdate(rows *sql.Rows, su *SendUpdate) {
	var remote int64
	err := rows.Scan(
		&su.Id,
		&su.Folder,
		&remote,
		&su.UpdateId)

	if err != nil {
		panic(err)
	}

	su.Remote = node.DeviceId(remote)
}
