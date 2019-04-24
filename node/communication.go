package node

import (
	"container/list"
	"errors"
	"log"
)

/**

提供Connection 通讯方面的实现
*/
var (
	ErrNoSuchConnection = errors.New("no such a connection for writing")
	ErrConnectionWrong  = errors.New("connection broken")
)

//todo 此处的锁策略是否会导致i/o效率下降 加锁是为了防止i/o的错乱
//SendMessage应该考虑到此时连接已不存在
func (cm *ConnectionNode) SendMessage(id DeviceId, msg interface{}) (funErr error) {
	defer func() {
		if funErr != nil {
			log.Printf("node module %s when send \n", funErr.Error())
		}
	}()
	cm.lock.Lock()
	if c, ok := cm.connsMap[id]; ok {
		cm.lock.Unlock()
		data, err := wrapMessage(msg)
		if err != nil {
			return err
		}
		_, err = c.s.Write(data)
		if err != nil {
			c.err = WrappedError{
				err,
				WriteOccasion,
			}
			log.Printf("%s inner error \n", err.Error())
			return ErrConnectionWrong
		}
		return nil
	} else {
		cm.lock.Unlock()
		return ErrNoSuchConnection
	}
}

/**
记下已注册的通知集合,一个通知激活后,是否该移除这个通知
*/
type NotificationSet struct {
	notifications map[DeviceId]*list.List
}

func newNs() *NotificationSet {
	ns := new(NotificationSet)
	ns.notifications = make(map[DeviceId]*list.List)
	return ns
}

//用于通知一个device conneciton 变得不可用
type ConnectionNotification struct {
	Remote DeviceId
	Ready  chan int
}

var (
	ErrNoAvailabelConn = errors.New("has not a availabhnm le connection")
)

func (ns *NotificationSet) alarmNotification(remote DeviceId) {
	if nl, ok := ns.notifications[remote]; ok {
		for e := nl.Front(); e != nil; {
			cno := e.Value.(*ConnectionNotification)
			cno.Ready <- 1
			old := e
			e = e.Next()
			nl.Remove(old)
		}
	}
}

func (cm *ConnectionNode) RegisterNotification(remote DeviceId) (*ConnectionNotification,
	error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	state := cm.connetionState(remote)
	if state != Connected {
		return nil, ErrNoAvailabelConn
	}
	var nl *list.List
	var ok bool
	if nl, ok = cm.ns.notifications[remote]; !ok {
		nl = list.New()
		cm.ns.notifications[remote] = nl
	}
	n := new(ConnectionNotification)
	n.Remote = remote
	n.Ready = make(chan int)
	nl.PushBack(n)
	return n, nil
}
