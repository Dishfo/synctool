package fs

import (
	"strconv"
	"sync"
	"syncfolders/bep"
)

/**
记录每一个 vector 对应的 文件真实修改时间 和文件记录中的修改时间
*/

type TimeTag struct {
	RealModTime     int64
	RecordedModTime int64
}

type CounterMap struct {
	counters sync.Map
}

func (vm *CounterMap) storeMap(v *bep.Counter, real, record int64) {
	if v == nil {
		return
	}
	key := createCounterString(v)
	vm.counters.Store(key, &TimeTag{
		RealModTime:     real,
		RecordedModTime: record,
	})
}

func (vm *CounterMap) getVectorTimes(v *bep.Counter) *TimeTag {
	key := createCounterString(v)
	tag, ok := vm.counters.Load(key)
	if ok {
		return tag.(*TimeTag)
	} else {
		return nil
	}
}

func createCounterString(c *bep.Counter) string {
	pre := strconv.FormatInt(int64(c.Id), 10)
	suf := strconv.FormatInt(int64(c.Value), 10)
	return pre + suf
}

//
func (fs *FileSystem) GetCounterTimeTag(folderId string, c *bep.Counter) *TimeTag {
	fs.lock.RLock()
	if fn, ok := fs.folders[folderId]; ok {
		fs.lock.RUnlock()
		return fn.counters.getVectorTimes(c)
	} else {
		fs.lock.RUnlock()
		return nil
	}
}
