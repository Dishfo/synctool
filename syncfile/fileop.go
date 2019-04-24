package syncfile

import (
	"encoding/base32"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syncfolders/bep"
	"syncfolders/fs"
	"time"
)

/**
提供同步事务中需要的文件操作
dup
copy
deleteFolder
createFile
createFolder
createLink
isExist() info
isNewer(os.info,bep.info)
restoreBak(fileBak)
*/

const (
	tmpFilePermission = 0775
)

const (
	TYPE_FILE = iota
	TYPE_DIR
	TYPE_LINK
)

type fileBak struct {
	file     string
	fileType int
	bak      string
	target   string //if it's a link file target is file name
}

func deleteFolder(folder string, needBak bool) (*fileBak, error) {
	if !needBak {
		deleteFolderWithOutBak(folder)
		return nil, nil
	} else {
		info, err := os.Stat(folder)
		if err != nil {
			return nil, err
		}

		if !info.IsDir() {
			return nil, errors.New("It's not a dir")
		}
		bak := new(fileBak)
		tmp := generateTmpName(folder)
		err = copyFolder(tmp, folder)
		if err != nil {
			return nil, err
		}
		bak.file = folder
		bak.bak = tmp
		bak.fileType = TYPE_DIR

		deleteFolderWithOutBak(folder)
		return bak, nil
	}
}

func deleteFolderWithOutBak(folder string) {
	infos, err := ioutil.ReadDir(folder)
	if err != nil {
		return
	}

	for _, info := range infos {
		filePath := filepath.Join(folder, info.Name())
		if info.IsDir() {
			deleteFolderWithOutBak(filePath)
		} else {
			_ = os.Remove(filePath)
		}
	}

	_ = os.Remove(folder)
}

func IsFile(info os.FileInfo) bool {
	if info.IsDir() {
		return false
	} else if IsLink(info) {
		return false
	}
	return true
}

func deleteFile(file string, needBak bool) (*fileBak, error) {
	if !needBak {
		_ = os.Remove(file)
		return nil, nil
	} else {
		info, err := os.Stat(file)
		if err != nil {
			return nil, err
		}

		if !IsFile(info) {
			return nil, errors.New("It's not normal file ")
		}

		bak := new(fileBak)
		tmp := generateTmpName(file)
		err = copyFile(tmp, file)
		if err != nil {
			return nil, err
		}
		bak.file = file
		bak.bak = tmp
		bak.fileType = TYPE_FILE
		_ = os.Remove(file)
		return bak, nil
	}
}

func deleteLink(link string, needBak bool) (*fileBak, error) {
	info, err := os.Stat(link)
	if err != nil {
		return nil, err
	}
	if !needBak {
		_ = os.Remove(link)
		return nil, nil
	} else {
		if !IsLink(info) {
			return nil, errors.New("It's not a link file ")
		}
		bak := new(fileBak)
		bak.target, _ = os.Readlink(link)
		bak.file = link
		bak.fileType = TYPE_LINK
		_ = os.Remove(link)
		return bak, nil
	}
}

func dupFile(dst, src string) (int64, error) {
	r, err := os.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	info, _ := r.Stat()
	w, err := os.OpenFile(dst, os.O_RDWR|os.O_TRUNC|os.O_CREATE,
		info.Mode())
	if err != nil {
		return 0, err
	}
	return io.Copy(w, r)
}

func deleteTarget(file string) {
	info, err := os.Stat(file)
	if err != nil {
		return
	}
	if info.IsDir() {
		deleteFolderWithOutBak(file)
	} else {
		_ = os.Remove(file)
	}
}

func copyFolder(dst, src string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return errors.New("It's not a dir ")
	}

	deleteFolderWithOutBak(dst)
	err = os.Mkdir(dst, info.Mode())
	srcInfos, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}

	links := make([]os.FileInfo, 0)

	for _, info := range srcInfos {
		srcPath := filepath.Join(src, info.Name())
		dstPath := filepath.Join(dst, info.Name())
		if info.IsDir() {
			err := copyFolder(dstPath, srcPath)
			if err != nil {
				return err
			}
		} else if IsLink(info) {
			links = append(links, info)
		} else {
			err := copyFile(dstPath, srcPath)
			if err != nil {
				return err
			}
		}
	}

	for _, info := range links {
		srcPath := filepath.Join(src, info.Name())
		target, _ := os.Readlink(srcPath)
		tName, _ := filepath.Rel(src, target)
		newlink := filepath.Join(dst, info.Name())
		newTarget := filepath.Join(dst, tName)
		err := os.Link(newTarget, newlink)
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(dst, src string) error {
	_, err := dupFile(dst, src)
	return err
}

//这个地方要是出现error了我dnmd
func restoreBak(bak *fileBak) {
	if bak == nil {
		return
	}

	info, err := os.Stat(bak.file)
	if err == nil {
		deleteTarget(info.Name())
	}

	//移除现有的文件
	switch bak.fileType {
	case TYPE_LINK:
		_ = os.Link(bak.target, bak.file)
	case TYPE_FILE:
		_ = copyFile(bak.file, bak.bak)
	case TYPE_DIR:
		_ = copyFolder(bak.file, bak.bak)
	}
}

func createFile(filePath string, mode os.FileMode) error {
	fPtr, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR,
		mode)
	if err != nil {
		return err
	}
	_ = fPtr.Close()
	return nil
}

func createFolder(filePath string, mode os.FileMode) error {
	return os.Mkdir(filePath, mode)
}

func createLink(filePath string, target string) error {
	return os.Link(target, filePath)
}

func hasNewerFile(info os.FileInfo, fileInfo *bep.FileInfo) bool {
	return info.ModTime().UnixNano() >
		(fileInfo.ModifiedS*fs.STons + int64(fileInfo.ModifiedNs))
}

const (
	tmpFilePrefix = "/tmp"
)

func generateTmpName(file string) string {
	name := fmt.Sprintf("%d%s", time.Now().UnixNano(), file)
	return filepath.Join(tmpFilePrefix, base32.StdEncoding.EncodeToString([]byte(name)))
}

func generateTmpFile(tFile *TargetFile, blockSet *BlockSet) (string, error) {
	filePath := fmt.Sprintf("%d%s%s",
		time.Now().UnixNano(), tFile.Folder, tFile.Name)
	filePath = base32.StdEncoding.EncodeToString([]byte(filePath))
	filePath = fmt.Sprintf("%s/%s", tmpFilePrefix, filePath)
	fPtr, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR,
		tmpFilePermission)
	if err != nil {
		return "", err
	}
	defer fPtr.Close()
	seqs := blockSet.fileBlockMap[tFile.Name]
	for _, seq := range seqs {
		block := blockSet.datas[seq]
		if block == nil {
			_ = os.Remove(filePath)
			return "", errors.New("data block is not available")
		}
		_, err := fPtr.Write(block)
		if err != nil {
			_ = os.Remove(filePath)
			return "", errors.New("generateTmp file failed ")
		}
	}
	return filePath, nil
}
