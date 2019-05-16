package fs

import (
	"os"
	"path/filepath"
	"syncfolders/bep"
)

type FolderNodeProxy struct {
	fn        *FolderNode
	folderId  string
	fileInfos []*bep.FileInfo
}

func (fp *FolderNodeProxy) StoreFileinfo(info *bep.FileInfo) {
	if info == nil {
		return
	}
	var real int64
	var record int64
	c := LastCounter(info)

	if info.Deleted {
		record = AllNsecond(info.ModifiedS, int64(info.ModifiedNs))
		real = record
	} else {
		filePath := filepath.Join(fp.fn.realPath,
			info.Name)
		record = AllNsecond(info.ModifiedS, int64(info.ModifiedNs))
		fInfo, err := os.Stat(filePath)
		if err == nil {
			real = fInfo.ModTime().UnixNano()
		} else {
			c = nil
		}
	}

	fp.fn.counters.storeMap(c, real, record)
	fp.fn.beforePushFileinfo(info)
	fp.fileInfos = append(fp.fileInfos, info)
}

func (fp *FolderNodeProxy) TryCommit() error {
	infoIds := make([]int64, 0)
	if len(fp.fileInfos) == 0 {
		return nil
	}
	tx, err := fp.fn.dw.GetTx()
	if err != nil {
		return err
	}

	for _, info := range fp.fileInfos {

		id, err := bep.StoreFileInfo(tx, fp.folderId, info)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		infoIds = append(infoIds, id)
	}

	indexSeq := IndexSeq{
		Folder: fp.folderId,
		Seq:    infoIds,
	}

	_, err = storeIndexSeq(tx, indexSeq)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (fn *FolderNode) NewFoldeProxy() *FolderNodeProxy {
	fp := new(FolderNodeProxy)
	fp.fn = fn
	fp.fileInfos = make([]*bep.FileInfo, 0)
	fp.folderId = fp.fn.folderId
	return fp
}
