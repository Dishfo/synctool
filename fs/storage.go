package fs

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"syncfolders/bep"
)

/**
将通	过sqlite 的方式记录
filelist(filelist 记录在内存中)
和对	应的fileInfo (记录在数据库中)
*/
//todo 重新设计接口

/**
todo 把tx 作为接口函数的参数
 这麽做可以在一组事务中保持一致性
 在生成 fileInfo ,可能 会获取最近的fileInfo
 基于上一次的version，追加一个新的 counter
 存储fileInfo后返回的 id ,
 将会被存储到indexSeq中,
 再把indexSeq 存储，
 这一组行为要保证全部完成的
 indexSeq 增加 folder字段 ,用于在查找时可以获取folder
 的所有更新
*/

/**
在程序中的对于文件的记录行为必然导致了延迟
缺乏对这些问题的探讨
*/

const (
	DbFileSuff          = ".db"
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

	createSeqTable = `
	create TABLE InfoSeq (
	id integer not null primary key autoincrement,
	seqs text,
	folder text
	)		
	`
)

type dbWrapper struct {
	db     *sql.DB
	dbFile string
}

func newDb(dbFile string) (*dbWrapper, error) {
	dw := new(dbWrapper)
	var err error
	db, err := sql.Open("sqlite3", dbFile)

	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createFileInfoTable)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createSeqTable)
	if err != nil {
		return nil, err
	}
	log.Println("init sqlite DataBase ")
	dw.db = db
	dw.dbFile = dbFile

	return dw, nil
}

func (dw *dbWrapper) Close() error {
	if dw.db == nil {
		return nil
	}
	defer func() {
		dw.db = nil
	}()
	return dw.db.Close()
}

func unmarshalBlcoks(p []byte) []*bep.BlockInfo {
	buf := bytes.NewBuffer(p)
	blocks := make([]*bep.BlockInfo, 0, 0)
	for {
		var l int64
		err := binary.Read(buf, binary.BigEndian, &l)
		if err != nil {
			break
		}
		b := make([]byte, l, l)
		_, _ = buf.Read(b)

		block := new(bep.BlockInfo)
		_ = proto.Unmarshal(b, block)
		blocks = append(blocks, block)
	}
	return blocks
}

func marshalBlcoks(blocks []*bep.BlockInfo) ([]byte, error) {
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

func (dw *dbWrapper) GetTx() (*sql.Tx, error) {
	return dw.db.Begin()
}
