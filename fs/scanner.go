package fs

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"syncfolders/bep"
)

/**
每隔一段时间扫描整个文件夹
计算出文件夹中遗漏的文件
在某些特定的情况下无法生成
具体的文件事件
组要用于寻找遗漏的文件
5分钟执行一次
获取
folderNode中的文件列表
与真实的文件列表进行比对
确认一组没有记录的文件并且
生成对应的fileInfo.
定期执行是对于文件update的更新
更新行为可以是基于file event的更新
也可以直接扫描文件对比update的更新
这里提供方式二的实现
两种方式不能同时并发的执行
扫描处理后应该移除某些对应的事件
--calculate update
----scanner Transcation
----fileEvent Transcation
*/

/**
扫描文件夹更新update
更新时会修改fileinfo记录
这是在更新时才会出现的写行为,(写入一系列的fileinfo 后，在写入一个indexSeq 表示update)
在外部设置fileinfo应该避免与模块内部的update事务穿插
更新期间停止了update的计算事务,
还需要等待当前计算事务的结束

交替的使用 internal fileinfo strore 会出现很严重的性能问题

*/

//scanner 还应该负责对修改时间与更新记录的事件不匹配的文件负责
/**
todo 对于文件夹相关的事件丢失暂时无法解决
*/
func (fn *FolderNode) scanFolderTransaction() {
	oldfiles := fn.fl.getItems()
	files := GetSubFiles(fn.realPath)
	notExist := findNotExist(oldfiles, files)
	vanished := findDisappear(oldfiles, files)

	infoIds := make([]int64, 0)
	infos := make([]*bep.FileInfo, 0)
	//find not exist
	tx, err := fn.dw.GetTx()
	if err != nil {
		return
	}
	for _, file := range notExist {

		if isHide(file) {
			continue
		}
		log.Printf("scanf %s", file)
		info := fn.createFileinfo(file)
		if info == nil {
			continue
		}

		if info.Type == bep.FileInfoType_DIRECTORY &&
			!fn.w.HasSubFolder(file) {
			fn.w.AppendWatcher(file)
		}

		id, err := bep.StoreFileInfo(tx, fn.folderId,
			info)
		if err != nil {
			log.Printf("%s when scanerr ", err.Error())
			_ = tx.Rollback()
			return
		}
		fn.cacheFileInfo(info)
		if info.Type == bep.FileInfoType_DIRECTORY {
			fn.fl.newFolder(file)
		} else {
			fn.fl.newFile(file)
		}

		infoIds = append(infoIds, id)
		infos = append(infos, info)
		//fn.discardEvent(file)
	}

	for _, file := range vanished {
		if fn.isInCounter(tx, file) {
			continue
		}
		//todo 是否应该生成删除记录
		fn.fl.removeItem(file)
	}

	indexSeq := new(IndexSeq)
	indexSeq.Folder = fn.folderId
	indexSeq.Seq = infoIds
	_ = tx.Commit()
	_, err = fn.internalStoreIndexSeq(indexSeq)
	for err != nil {
		_, err = fn.internalStoreIndexSeq(indexSeq)
	}
}

func (fn *FolderNode) createFileinfo(file string) *bep.FileInfo {
	name, _ := filepath.Rel(fn.realPath, file)
	info, err := GenerateFileInfo(file)
	if err != nil {
		return nil
	}
	tx, err := fn.dw.GetTx()
	if err != nil {
		return nil
	}
	defer tx.Commit()
	err, c := fn.appendFileInfo(tx, info)
	if err != nil {
		return nil
	}
	info.Name = name
	info.ModifiedBy = uint64(LocalUser)
	fn.counters.storeMap(c,
		AllNsecond(info.ModifiedS, int64(info.ModifiedNs)),
		AllNsecond(info.ModifiedS, int64(info.ModifiedNs)))
	return info
}

//返回一个由文件绝对路劲构成的切片
func findNotExist(oldfiles, files []string) []string {
	oldfilesMap := make(map[string]bool)
	notExist := make([]string, 0)
	for _, f := range oldfiles {
		oldfilesMap[f] = true
	}

	for _, f := range files {
		if !oldfilesMap[f] {
			notExist = append(notExist, f)
		}
	}

	return notExist
}

func findDisappear(oldfiles, files []string) []string {
	newFilesMap := make(map[string]bool)
	vanished := make([]string, 0)
	for _, f := range files {
		newFilesMap[f] = true
	}

	for _, f := range oldfiles {
		if !newFilesMap[f] {
			vanished = append(vanished, f)
		}
	}
	return vanished
}

//findNeedUpdateFiles 用于寻找update 记录的修改时间，与文件实际修改时间不一致的文件
func (fn *FolderNode) findNeedUpdateFiles(exists []string,
	tx *sql.Tx) []string {
	files := make([]string, 0)
	var errno error
	if errno != nil {
		return files
	}

	for _, name := range exists {
		relName, _ := filepath.Rel(fn.realPath, name)
		info, err := bep.GetRecentInfo(tx, fn.folderId, relName)
		if err != nil {
			continue
		}

		if info == nil {
			files = append(files, name)
		} else {
			oif, err := os.Stat(name)
			if err != nil {
				continue
			}
			if !checkFileOutDate(oif, info) {
				files = append(files, name)
			}
		}
	}

	return files
}

func checkFileOutDate(oif os.FileInfo, info *bep.FileInfo) bool {
	fileType := bepFileType(oif)
	if fileType != info.Type {
		return false
	}

	if fileType == bep.FileInfoType_DIRECTORY {
		return true
	}

	if oif.ModTime().UnixNano() !=
		info.ModifiedS*STons+int64(info.ModifiedNs) {
		return false
	}

	return true
}

func bepFileType(oif os.FileInfo) bep.FileInfoType {
	mod := oif.Mode()
	if mod.IsDir() {
		return bep.FileInfoType_DIRECTORY
	} else if mod&os.ModeSymlink != 0 {
		return bep.FileInfoType_SYMLINK
	}
	return bep.FileInfoType_FILE
}

//name 是文件的绝对路径
func (fn *FolderNode) discardEvent(name string) {
	delete(fn.eventSet.lists, name)
}

//todo 出现了多次的类似函数 我总有一天要把他们整合起来
func GetSubFiles(folder string) []string {
	files := make([]string, 0)
	infos, err := ioutil.ReadDir(folder)
	if err != nil {
		return files
	}

	for _, info := range infos {
		if isHide(info.Name()) {
			continue
		}
		filePath := filepath.Join(folder, info.Name())
		files = append(files, filePath)
		if info.IsDir() {
			files = append(files, GetSubFiles(filePath)...)
		}
	}

	return files
}

//表示当前事务的开始
func (fn *FolderNode) startUpdate() bool {
	select {
	case fn.updateFlag <- 1:
	default:
		return false
	}
	return true
}

//表示事务的结束
func (fn *FolderNode) endUpdate() {
	select {
	case <-fn.updateFlag:
	}
}

/**
上面三个函数是相关标记位的读写接口
*/
