package bep

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"log"
)

/**
提供fileinfo的存储接口
*/

const (
	createFileInfoTable = `
	create table FileInfo (
	id integer not null primary key autoincrement,
	folder text,
	Name text,
	type integer,
	size integer,
	permissions integer,
	modifiedby integer,
	modifieds  integer,
	modifiedNs integer,
	deleted    integer,
	invaild    integer,
	nopermissions  integer,
	version     blob,	
	blocksize   integer,
	blocks      blob,
	linktarget  text
  	)	
	`

	fileInfoTableField = `
	 Name,type,size,permissions,modifiedby,modifieds,
	modifiedNs,deleted,invaild,nopermissions,
	version,blocksize,blocks,linktarget
	`

	selectInfo = `
	select ` + fileInfoTableField + ` from FileInfo where Name = ? and folder = ?  
	order by id desc 
	limit 1`

	selectInfos = `
	select ` + fileInfoTableField + ` from FileInfo where folder = ? and Name = ? 
	order by id desc`

	selectInfoById = `
	select ` + fileInfoTableField + `from  FileInfo where id = ?
	`

	selectRecentVersion = `
  	select version 
	 from FileInfo 
	 where  folder = ? and Name = ? 
	 order by id desc  limit 1
	`
	updateInfo = `
	update FileInfo set invaild = ? where folder = ? and Name = ? and id in (
		select f2.id from FileInfo f2  where f2.folder = folder and f2.Name = Name 	
		order by id desc 
		limit 1
	)
	`

	insertFileInfo = `
	insert into FileInfo
	(folder,Name,type,size,permissions,modifiedby,modifieds,
	modifiedNs,deleted,invaild,nopermissions,
	version,blocksize,blocks,linktarget) 
	values(?,?,?,?,?,?,?
	,?,?,?,?,
	?,?,?,?)	
	`
)

func CreateFileInfoTable(db *sql.DB) (sql.Result, error) {
	return db.Exec(createFileInfoTable)
}

func GetRecentInfo(tx *sql.Tx, folder, name string) (*FileInfo, error) {

	stmt, err := tx.Prepare(selectInfo)
	if err != nil {
		log.Printf("%s when select recent fileinfo ", err.Error())
		return nil, err
	}
	res, err := stmt.Query(name, folder)
	if err != nil {
		log.Printf("%s when select recent fileinfo ", err.Error())
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		info := new(FileInfo)
		fillFileInfo(res, info)
		return info, nil
	} else {
		return nil, nil
	}
}

func GetRecentVersion(tx *sql.Tx, folder, name string) (*Vector, error) {
	rows, err := tx.Query(selectRecentVersion, folder, name)
	if err != nil {
		log.Printf("%s when get recent version of %s %s",
			err.Error(), folder, name)
		return nil, err
	}
	defer rows.Close()
	c := new(Vector)
	if rows.Next() {

		var p []byte
		_ = rows.Scan(&p)
		_ = proto.Unmarshal(p, c)
		return c, nil
	} else {
		c.Counters = make([]*Counter, 0)
		return c, nil
	}
}

//获取指定 id的 fileinfo
func GetInfoById(tx *sql.Tx, id int64) (*FileInfo, error) {

	res, err := tx.Query(selectInfoById, id)
	if err != nil {
		log.Printf("%s when select fileinfo by %d", err.Error(), id)
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		info := new(FileInfo)
		fillFileInfo(res, info)
		return info, nil
	} else {
		return nil, nil
	}
}

//将一个文件设置为 invaild 处于无法提供数据的状态
func SetInvalid(tx *sql.Tx, folder, name string) (int64, error) {
	res, err := tx.Exec(updateInfo, 1, folder, name)
	if err != nil {
		log.Printf("%s when set invaild flag for %s %s ",
			err.Error(), folder, name)
		return -1, err
	}
	id, _ := res.RowsAffected()

	return id, nil
}

//存储一个 fileinfo 并返回 id
func StoreFileInfo(tx *sql.Tx, folder string, info *FileInfo) (int64, error) {
	p, err := marshalBlcoks(info.Blocks)
	version, err := proto.Marshal(info.Version)
	if err != nil {
		log.Printf("%s when marshal version", err.Error())
		return -1, err
	}

	res, err := tx.Exec(insertFileInfo,
		folder, info.Name,
		info.Type,
		info.Size,
		info.Permissions,
		int64(info.ModifiedBy),
		info.ModifiedS,
		info.ModifiedNs,
		info.Deleted,
		info.Invalid,
		info.NoPermissions,
		version,
		info.BlockSize,
		p,
		info.SymlinkTarget,
	)

	if err != nil {
		log.Printf("%s cp when insert a fileinfo %s %s ",
			err.Error(), folder, info.Name)
		return -1, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func GetFileInfoByName(tx *sql.Tx, folderId, name string) (*FileInfo, error) {
	stmt, err := tx.Prepare(selectInfo)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(folderId, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		info := new(FileInfo)
		fillFileInfo(rows, info)
		return info, nil
	}
	return nil, nil
}

//
//func IsInvalid(tx *sql.Tx, folderId, name string) bool {
//
//}

func GetFileInfos(tx *sql.Tx, ids []int64) ([]*FileInfo, error) {
	fileInfos := make([]*FileInfo, 0)
	for _, id := range ids {
		fileinfo, err := GetInfoById(tx, id)
		if err != nil {
			return nil, err
		}
		if fileinfo == nil {
			continue
		}
		fileInfos = append(fileInfos, fileinfo)
	}
	return fileInfos, nil
}

func fillFileInfo(rows *sql.Rows, info *FileInfo) {
	var version []byte
	var b []byte
	if info == nil {
		return
	}
	var tmp int64
	err := rows.Scan(&info.Name,
		&info.Type,
		&info.Size,
		&info.Permissions,
		&tmp,
		&info.ModifiedS,
		&info.ModifiedNs,
		&info.Deleted,
		&info.Invalid,
		&info.NoPermissions,
		&version,
		&info.BlockSize,
		&b,
		&info.SymlinkTarget)
	if err != nil {
		log.Fatalf("%s when fill fileInfo", err.Error())
	}

	info.ModifiedBy = uint64(tmp)
	info.Version = new(Vector)
	err = proto.Unmarshal(version, info.Version)
	if err != nil {
		log.Fatalf("%s when fill fileInfo ", err.Error())
	}

	info.Blocks = unmarshalBlcoks(b)
}

func unmarshalBlcoks(p []byte) []*BlockInfo {
	buf := bytes.NewBuffer(p)
	blocks := make([]*BlockInfo, 0, 0)
	for {
		var l int64
		err := binary.Read(buf, binary.BigEndian, &l)
		if err != nil {
			break
		}
		b := make([]byte, l, l)
		_, _ = buf.Read(b)

		block := new(BlockInfo)
		_ = proto.Unmarshal(b, block)
		blocks = append(blocks, block)
	}
	return blocks
}

func marshalBlcoks(blocks []*BlockInfo) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	for _, b := range blocks {
		if p, err := proto.Marshal(b); err == nil {
			l := int64(len(p))
			_ = binary.Write(buf, binary.BigEndian, l)
			buf.Write(p)
		} else {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
