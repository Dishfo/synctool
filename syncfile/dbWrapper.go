package syncfile

import "log"

/**
对数据库的读写函数进行包装
某些操作不需要依赖外部传递的事务
有的操作如果不能成功就会导致后续的逻辑无法进行
*/

const (
	maxRetryTime = 15
)

//storeSendUpdate 在发送update 后调用，需要保证这个函数成功
func (sm *SyncManager) storeSendUpdate(su *SendUpdate) {
	err := sm.internalStoreSendUpdate(su)
	maxTime := maxRetryTime
	for err != nil && maxTime > 0 {
		err = sm.internalStoreSendUpdate(su)
		maxTime -= 1
	}

	if err != nil {
		log.Fatal(err, " when save record send update ")
	}
}

func (sm *SyncManager) internalStoreSendUpdate(su *SendUpdate) error {
	tx, err := sm.cacheDb.Begin()
	if err != nil {
		return err
	}
	_, err = storeSendUpdate(tx, su)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	_ = tx.Commit()
	return nil
}
