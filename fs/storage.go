package fs

import (
	"database/sql"
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
	DbFileSuff = ".db"

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

	_, err = bep.CreateFileInfoTable(db)
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
	_, err = dw.db.Exec("PRAGMA mmap_size=268435456")
	if err != nil {
		return nil, err
	}

	_, err = dw.db.Exec("PRAGMA cache_size=268435456")
	if err != nil {
		return nil, err
	}

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

func (dw *dbWrapper) GetTx() (*sql.Tx, error) {
	return dw.db.Begin()
}
