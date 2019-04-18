package fs

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/base32"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syncfolders/bep"
	"time"
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
	name text,
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

var (
	db *sql.DB
)

func init() {
	if db != nil {
		return
	}

	removeOldDatabase()
	var dbname = randomDbName()
	var err error
	db, err = sql.Open("sqlite3", dbname)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(createFileInfoTable)
	if err != nil {
		log.Fatal(err, " when create fileInfo table")
	}

	_, err = db.Exec(createSeqTable)
	if err != nil {
		log.Fatal(err, " when create fileInfo table")
	}
	log.Println("init sqlite DataBase ")

}

type fileList struct {
	folder string
	real   string
	items  map[string]int
	lock   sync.RWMutex
	ready  chan int
}

/**
todo 事务处理中应该有更加细致的隔离等级划分
*/
func GetTx() (*sql.Tx, error) {
	return db.Begin()
}

func newFileList(folder string) *fileList {
	fl := new(fileList)
	fl.folder = folder
	fl.items = make(map[string]int)
	fl.ready = make(chan int)
	return fl
}

func (fl *fileList) getItems() []string {
	items := make([]string, 0)
	for k, v := range fl.items {
		if v > 0 {
			items = append(items, k)
		}
	}
	return items
}

func (fl *fileList) newItem(name string) {
	fl.items[name] = 1
}

func (fl *fileList) removeItem(name string) {
	fl.items[name] = 0
}

func Close() {
	_ = db.Close()
}

func removeOldDatabase() {
	var dir, _ = os.Getwd()
	var files, _ = ioutil.ReadDir(dir)
	for _, file := range files {
		if filepath.Ext(file.Name()) == DbFileSuff {
			_ = os.Remove(filepath.Join(dir, file.Name()))
		}
	}
}

func randomDbName() string {
	var prefix = strconv.FormatInt(time.Now().Unix(), 10)
	var suffix = strconv.FormatInt(time.Now().UnixNano(), 10)
	var name = prefix + suffix
	var hash = md5.Sum([]byte(name))
	name = base32.StdEncoding.EncodeToString(hash[:])
	return name + DbFileSuff
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
