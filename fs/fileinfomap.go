package fs

import (
	"sync"
	"syncfolders/bep"
)

/**
提供一个一个映射表
对应了每一个 文件的已知fileinfo 最近存储的一个
set invalid 将会在这里设置
get invalid 也会在这里获取
*/

type fileInfoMap struct {
	fileInfos map[string]*bep.FileInfo
	lock      sync.RWMutex
}

func newFileInCache() *fileInfoMap {
	fms := new(fileInfoMap)
	fms.fileInfos = make(map[string]*bep.FileInfo)
	return fms
}

func (fms *fileInfoMap) getFileInfo(name string) *bep.FileInfo {
	fms.lock.RLock()
	defer fms.lock.RUnlock()
	return fms.fileInfos[name]
}

func (fms *fileInfoMap) cacheFileInfo(name string, info *bep.FileInfo) {
	fms.lock.Lock()
	defer fms.lock.Unlock()
	fms.fileInfos[name] = info
}

func (fms *fileInfoMap) setInvalid(name string) {
	fms.lock.Lock()
	defer fms.lock.Unlock()
	if info, ok := fms.fileInfos[name]; ok {
		info.Invalid = true
	}
}

func (fms *fileInfoMap) isInvalid(name string) bool {
	fms.lock.RLock()
	defer fms.lock.RUnlock()
	if info, ok := fms.fileInfos[name]; ok {
		return info.Deleted
	}
	return false
}

//todo 针对函数
