package fs

import (
	"crypto/md5"
	"database/sql"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"log"
	"math"
	"os"
	"syncfolders/bep"
	"synctool/util"
)

/**
存储 fileinfo
并把相关的fileInfo Id 记录在 update 或 index 里面
*/

const (
	fileInfoTableField = `
	 name,type,size,permissions,modifiedby,modifieds,
	modifiedNs,deleted,invaild,nopermissions,
	version,blocksize,blocks,linktarget
	`

	selectInfo = `
	select ` + fileInfoTableField + ` from FileInfo where folder = ? and name = ? 
	order by id desc 
	limit 1`

	selectInfos = `
	select ` + fileInfoTableField + ` from FileInfo where folder = ? and name = ? 
	order by id desc`

	selectInfoById = `
	select ` + fileInfoTableField + `from  FileInfo where id = ?
	`

	selectRecentVersion = `
  	select version 
	 from FileInfo 
	 where  folder = ? and name = ? 
	 order by id desc  limit 1
	`
	updateInfo = `
	update FileInfo set invaild = ? where folder = ? and name = ? and id in (
		select f2.id from FileInfo f2  where f2.folder = folder and f2.name = name 	
		order by id desc 
		limit 1
	)
	`

	insertFileInfo = `
	insert into FileInfo
	(folder,name,type,size,permissions,modifiedby,modifieds,
	modifiedNs,deleted,invaild,nopermissions,
	version,blocksize,blocks,linktarget) 
	values(?,?,?,?,?,?,?
	,?,?,?,?,
	?,?,?,?)	
	`
)

func GetRecentInfo(tx *sql.Tx, folder, name string) (*bep.FileInfo, error) {
	res, err := tx.Query(selectInfo, folder, name)
	if err != nil {
		log.Printf("%s when select recent fileinfo ", err.Error())
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		info := new(bep.FileInfo)
		fillFileInfo(res, info)
		return info, nil
	} else {
		return nil, nil
	}
}

func GetRecentVersion(tx *sql.Tx, folder, name string) (*bep.Vector, error) {
	rows, err := tx.Query(selectRecentVersion, folder, name)
	if err != nil {
		log.Printf("%s when get recent version of %s %s",
			err.Error(), folder, name)
		return nil, err
	}
	defer rows.Close()
	c := new(bep.Vector)
	if rows.Next() {

		var p []byte
		_ = rows.Scan(&p)
		_ = proto.Unmarshal(p, c)
		return c, nil
	} else {
		c.Counters = make([]*bep.Counter, 0)
		return c, nil
	}
}

//获取指定 id的 fileinfo
func GetInfoById(tx *sql.Tx, id int64) (*bep.FileInfo, error) {

	res, err := tx.Query(selectInfoById, id)
	if err != nil {
		log.Printf("%s when select fileinfo by %d", err.Error(), id)
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		info := new(bep.FileInfo)
		fillFileInfo(res, info)
		return info, nil
	} else {
		return nil, nil
	}
}

//将一个文件设置为 invaild 处于无法提供数据的状态
func SetInvaild(tx *sql.Tx, folder, name string) (int64, error) {
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
func StoreFileinfo(tx *sql.Tx, folder string, info *bep.FileInfo) (int64, error) {
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
		log.Printf("%s when insert a fileinfo %s %s ",
			err.Error(), folder, info.Name)
		return -1, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func GetFileInfos(tx *sql.Tx, ids []int64) ([]*bep.FileInfo, error) {
	fileInfos := make([]*bep.FileInfo, 0)
	for _, id := range ids {
		fileinfo, err := GetInfoById(tx, id)
		if err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, fileinfo)
	}
	return fileInfos, nil
}

func GetIndex(tx *sql.Tx, indexSeq *IndexSeq) (*bep.Index, error) {
	index := new(bep.Index)
	infos, err := GetFileInfos(tx, indexSeq.Seq)
	if err != nil {
		return nil, err
	}

	index.Folder = indexSeq.Folder
	index.Files = infos

	return index, nil
}

func GetIndexUpdate(tx *sql.Tx, updateSeq *IndexSeq) (*bep.IndexUpdate, error) {
	update := new(bep.IndexUpdate)
	infos, err := GetFileInfos(tx, updateSeq.Seq)
	if err != nil {
		return nil, err
	}
	update.Folder = updateSeq.Folder
	update.Files = infos

	return update, nil
}

//rows 中的内容提取到 info 中
func fillFileInfo(rows *sql.Rows, info *bep.FileInfo) {
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
	info.Version = new(bep.Vector)
	err = proto.Unmarshal(version, info.Version)
	if err != nil {
		log.Fatalf("%s when fill fileInfo ", err.Error())
	}

	info.Blocks = unmarshalBlcoks(b)
}

func GernerateFileInfoInDel(folder, name string, e WrappedEvent) (*bep.FileInfo, error) {
	tx, err := GetTx()
	if err != nil {
		_ = tx.Rollback()
		return nil, errDbWrong
	}
	info, err := GetRecentInfo(tx, folder, name)
	if err != nil {
		_ = tx.Rollback()
		return nil, errDbWrong
	}
	if info == nil {
		_ = tx.Rollback()
		return nil, errNoNeedInfo
	}
	_ = tx.Commit()
	info.Deleted = true
	info.Size = 0
	info.ModifiedS = e.Mods
	info.ModifiedNs = int32(e.ModNs - STons*e.Mods)

	return info, nil
}

//根据提供的文件绝对路径输出一个 fileinfo 指针
func GenerateFileInfo(file string) (*bep.FileInfo, error) {
	finfo, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	mode := finfo.Mode()
	if mode.IsDir() {
		return generateFileInforDir(file)
	} else if mode&os.ModeSymlink != 0 {
		return generateFileInforLink(file)
	} else {
		return generateFileInfo(file)
	}

}

func generateFileInforDir(file string) (*bep.FileInfo, error) {
	info := new(bep.FileInfo)
	finfo, _ := os.Stat(file)
	info.Permissions = uint32(finfo.Mode().Perm())
	info.Type = bep.FileInfoType_DIRECTORY

	return info, nil
}

func generateFileInforLink(file string) (*bep.FileInfo, error) {
	info := new(bep.FileInfo)
	finfo, _ := os.Stat(file)
	info.Permissions = uint32(finfo.Mode().Perm())
	info.Type = bep.FileInfoType_SYMLINK
	target, err := os.Readlink(file)
	if err != nil {
		return nil, err
	}
	info.SymlinkTarget = target
	return info, nil
}

func generateFileInfo(file string) (*bep.FileInfo, error) {
	info := new(bep.FileInfo)
	finfo, _ := os.Stat(file)
	info.Size = finfo.Size()
	info.BlockSize = selectBlockSize(info.Size)
	info.ModifiedS = finfo.ModTime().Unix()
	info.ModifiedNs = int32(finfo.ModTime().UnixNano() - 1000000000*info.ModifiedS)
	info.Permissions = uint32(finfo.Mode().Perm())
	info.ModifiedBy = uint64(LocalUser)
	info.Type = bep.FileInfoType_FILE
	blocks, err := caculateBlocks(file, info.BlockSize)
	if err != nil {
		return nil, err
	}

	info.Blocks = blocks
	return info, nil
}

func caculateBlocks(file string, bsize int32) ([]*bep.BlockInfo, error) {
	var (
		offset   int64 = 0
		filesize int64 = 0
		bend     int64 = 0
	)
	data, err := ioutil.ReadFile(file)

	if err != nil {
		return nil, err
	}

	filesize = int64(len(data))
	blocks := make([]*bep.BlockInfo, 0, 5)
	for ; offset < filesize; offset = offset + int64(bsize) {
		b := new(bep.BlockInfo)
		if filesize-offset < int64(bsize) {
			bend = filesize - offset
		} else {
			bend = int64(bsize)
		}

		b.Size = int32(bend)
		b.Offset = offset
		b.WeakHash = util.Adler(data[offset : offset+bend])
		md5hash := md5.Sum(data[offset : offset+bend])
		b.Hash = md5hash[:]
		blocks = append(blocks, b)
	}

	return blocks, nil
}

const (
	_ = 1 << (10 * iota)
	KB
	MB
	GB
)

var (
	selectionBsize = []int32{
		128 * KB,
		256 * KB,
		512 * KB,
		1 * MB,
		2 * MB,
		4 * MB,
		8 * MB,
		16 * MB,
	}
)

func selectBlockSize(size int64) int32 {
	index := math.Log2(float64(size))
	if index > 27 {
		return selectionBsize[int(index)-27]
	} else {
		return selectionBsize[0]
	}
}
