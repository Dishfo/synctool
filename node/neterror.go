package node

import "fmt"

/**
当在一个stream 进行读写操作如果发生错误
把错误记录下来等待解决
 */
const (
	ReadOccasion = iota
	WriteOccasion
)

type WrappedError struct {
	source   error
	occasion int
}

func (err WrappedError) Error() string {
	return fmt.Sprintf("%s on %d", err.source.Error(), err.occasion)
}

//todo 可能要移除这个函数了
func (cm *ConnectionNode) HandleErr(id DeviceId, method func(err error, occa int)) {
	cm.lock.Lock()
	if c, ok := cm.connsMap[id]; ok {
		c.lock.Lock()
		defer c.lock.Unlock()
		cm.lock.Unlock()
		if c.err == nil {
			return
		}
		wrappedError := c.err.(WrappedError)
		method(wrappedError.source, wrappedError.occasion)
		c.err = nil
	} else {
		cm.lock.Unlock()
	}
}
