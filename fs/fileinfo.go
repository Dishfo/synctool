package fs

import (
	"crypto/md5"
	"database/sql"
	"io/ioutil"
	"math"
	"os"
	"syncfolders/bep"
	"synctool/util"
)

/**
存储 fileinfo
并把相关的fileInfo Id 记录在 update 或 index 里面
*/

func getIndex(tx *sql.Tx, indexSeq *IndexSeq) (*bep.Index, error) {
	index := new(bep.Index)
	infos, err := bep.GetFileInfos(tx, indexSeq.Seq)
	if err != nil {
		return nil, err
	}

	index.Folder = indexSeq.Folder
	index.Files = infos

	return index, nil
}

func getIndexUpdate(tx *sql.Tx, updateSeq *IndexSeq) (*bep.IndexUpdate, error) {
	update := new(bep.IndexUpdate)
	infos, err := bep.GetFileInfos(tx, updateSeq.Seq)
	if err != nil {
		return nil, err
	}
	update.Folder = updateSeq.Folder
	update.Files = infos

	return update, nil
}

//根据提供的文件绝对路径输出一个 fileinfo 指针
func GenerateFileInfo(file string) (*bep.FileInfo, error) {
	fInfo, err := os.Stat(file)
	if err != nil {
		return nil, err
	}

	mode := fInfo.Mode()
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
	fInfo, _ := os.Stat(file)
	info.Permissions = uint32(fInfo.Mode().Perm())
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
