package fs

import (
	"log"
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
	log.Println(indexSeq.Seq)
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
