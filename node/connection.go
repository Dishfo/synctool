package node

import (
	"fmt"
	"github.com/libp2p/go-libp2p-net"
	"github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/pkg/errors"
	"log"
	"sync"
	"syncfolders/bep"
)

/**
提供节点间的连接
*/
//todo 及时通知某个设备间的连接不可用了
/*

 */
var (
	//由主模块进行注册
	DeviceName    = ""
	ClientName    = ""
	ClientVersion = ""
)

const BepProtocol = "bep1.0.0"

var (
	ErrHasConnected = errors.New("Has a Connected with device")
	ErrIsConnecting = errors.New("now,is connecting with device")
)

type ConnectState int

const (
	NoneState ConnectState = iota
	Connecting
	Connected
)

//对节点间的连接进行描述
type ConnectionFd struct {
	state  ConnectState
	remote DeviceId
}

func (cfd *ConnectionFd) State() ConnectState {
	return cfd.state
}

func (cfd *ConnectionFd) SetState(state ConnectState) {
	cfd.state = state
}

//hold the stream and  error
type Connection struct {
	s      net.Stream
	remote DeviceId
	err    error
	lock   sync.Mutex
}

func (c *Connection) setError(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.err = err
}

func (c *Connection) getStream() net.Stream {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.s
}

func (c *Connection) remoteID() peer.ID {
	return c.s.Conn().RemotePeer()
}

type ConnectionNode struct {
	*BaseNode
	lock       sync.RWMutex
	connStates map[DeviceId]*ConnectionFd
	nameMap    sync.Map //来自外部的参数,并不一定可靠
	connsMap   map[DeviceId]*Connection
	msgs       chan WrappedMessage

	ns *NotificationSet
}

//create a new node with connection and send function
func NewConnectionNode(configs map[string]string) (*ConnectionNode, error) {
	cn := new(ConnectionNode)
	n, err := NewNode(configs)
	if err != nil {
		return nil, err
	}
	cn.BaseNode = n
	cn.connsMap = make(map[DeviceId]*Connection)
	cn.connStates = make(map[DeviceId]*ConnectionFd)
	cn.msgs = make(chan WrappedMessage)
	cn.setStreamHandler()
	cn.ns = newNs()
	return cn, nil
}

func (cm *ConnectionNode) DeviceAddrs(id DeviceId) []string {
	if c, ok := cm.connsMap[id]; ok {
		remoteId := c.remoteID()
		addrs := cm.h.Peerstore().Addrs(remoteId)
		addrstrs := make([]string, 0)
		for _, addr := range addrs {
			addrstrs = append(addrstrs, addr.String())
		}
		return addrstrs
	} else {
		return []string{}
	}
}

//连接特定的设备，失败后 返回error
func (cm *ConnectionNode) ConnectDevice(id DeviceId, hostId string) error {

	cstate := cm.checkCfd(id)
	if cstate == Connecting {
		return ErrIsConnecting
	}

	if cstate == Connected {
		return ErrHasConnected
	}

	return cm.tryConnecting(id, hostId)
}

func (cm *ConnectionNode) Messages() chan WrappedMessage {
	return cm.msgs
}

func (cm *ConnectionNode) checkCfd(id DeviceId) ConnectState {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	if v, ok := cm.connStates[id]; ok {
		return v.state
	} else {
		return NoneState
	}
}

//居然把查找节点 address 的事给忘了
func (cm *ConnectionNode) tryConnecting(id DeviceId, hostId string) error {

	cm.lock.Lock()
	cm.nameMap.Store(id, hostId)
	succed := false
	hId, err := peer.IDB58Decode(hostId)
	if err != nil {
		cm.lock.Unlock()
		return errors.New("invaild host id " + hostId)
	}
	if _, ok := cm.connStates[id]; ok {
		return ErrIsConnecting
	}

	cm.connStates[id] = new(ConnectionFd)
	cm.connStates[id].SetState(Connecting)
	cm.lock.Unlock()
	defer func() {
		if !succed {
			cm.lock.Lock()
			delete(cm.connStates, id)
			cm.lock.Unlock()
		}
	}()
	pinfo, err := cm.dhtPeer.FindPeer(cm.ctx, hId)
	if err != nil {
		return fmt.Errorf("%s when find peer %s\n", err.Error(), hostId)
	}

	if len(pinfo.Addrs) == 0 {
		return fmt.Errorf("can't find  peer %s\n", hostId)
	}

	cm.h.Peerstore().AddAddrs(pinfo.ID,
		pinfo.Addrs, peerstore.PermanentAddrTTL)

	s, err := cm.h.NewStream(cm.ctx, hId, BepProtocol)

	defer func() {
		if !succed {
			if s != nil {
				_ = s.Close()
			}
		}
	}()

	if err != nil {
		return fmt.Errorf("occur %s when create stream to %s ", err.Error(),
			id.String())
	}

	hello := getHello()
	err = sendHello(s, hello)
	if err != nil {
		return fmt.Errorf("occur %s when send hello to %s ", err,
			id.String())
	}

	rhello, err := receiveHello(s)
	if err != nil {
		return fmt.Errorf("occur %s when receive hello to %s ", err,
			id.String())
	}

	cm.lock.Lock()
	defer cm.lock.Unlock()
	if confirmHello(rhello) {
		succed = true
		c := new(Connection)
		c.s = s
		c.remote = id
		cm.connsMap[id] = c
		cm.connStates[id].state = Connected
		cm.afterEstablishConnection(c)
	}

	return nil
}

//断开与某一特定设备连接
func (cm *ConnectionNode) DisConnect(id DeviceId) {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	delete(cm.connStates, id)
	cm.disConnectionReal(id)
}

func (cm *ConnectionNode) ConnectionState(id DeviceId) ConnectState {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	return cm.connetionState(id)
}

func (cm *ConnectionNode) connetionState(id DeviceId) ConnectState {
	if s, ok := cm.connStates[id]; ok {
		return s.state
	} else {
		return NoneState
	}
}

func (cm *ConnectionNode) disConnectionReal(id DeviceId) {
	if c, ok := cm.connsMap[id]; ok {
		if c == nil {
			return
		}
		_ = c.getStream().Close()
		delete(cm.connsMap, id)
		cm.ns.alarmNotification(id)
	}
}

func (cm *ConnectionNode) Close() {
	cm.BaseNode.Close()

}

type HandShakeHandler interface {
	ConfirmHello(hello *bep.Hello) bool
}

var (
	//由主模块来注册
	registerHandShaker HandShakeHandler = nil
)

func RegisterHandShake(handler HandShakeHandler) {
	registerHandShaker = handler
}

func confirmHello(hello *bep.Hello) bool {
	if hello == nil {
		return false
	}

	if registerHandShaker == nil {
		log.Println("always return fasle because of " +
			"null register handler")
		return false
	}

	return registerHandShaker.ConfirmHello(hello)
}

func getHello() *bep.Hello {
	return &bep.Hello{
		DeviceName:    DeviceName,
		ClientVersion: ClientVersion,
		ClientName:    ClientName,
	}
}

/**
为host设置stream handler 在对方主动发起连接时
我们可以正常的进行握手
*/
func (cm *ConnectionNode) setStreamHandler() {
	cm.h.SetStreamHandler(BepProtocol, func(stream net.Stream) {
		var devId DeviceId
		hostId := stream.Conn().RemotePeer().Pretty()
		contain := false
		cm.nameMap.Range(func(key, value interface{}) bool {
			hid := value.(string)
			if hid == hostId {
				devId = key.(DeviceId)
				contain = true
				return false
			}
			return true
		})

		if contain {
			cm.handkShake(stream, devId)
		} else {
			log.Printf("a unknow host %s \n", hostId)
			_ = stream.Close()
		}

	})
}

func (cm *ConnectionNode) handkShake(stream net.Stream, id DeviceId) {
	cm.lock.Lock()
	if _, ok := cm.connStates[id]; ok {
		_ = stream.Close()
		return
	}
	cm.connStates[id] = new(ConnectionFd)
	cm.connStates[id].SetState(Connecting)
	cm.lock.Unlock()
	hello, err := receiveHello(stream)
	if err != nil {
		log.Printf("%s when handshake %s", err.Error(), id.String())
		cm.lock.Lock()
		_ = stream.Close()
		delete(cm.connStates, id)
		cm.lock.Unlock()
		return
	}

	if confirmHello(hello) {
		err := sendHello(stream, getHello())
		cm.lock.Lock()
		defer cm.lock.Unlock()
		if err != nil {
			_ = stream.Close()
			delete(cm.connStates, id)
		}
		c := new(Connection)
		c.s = stream
		c.remote = id
		cm.connsMap[id] = c
		cm.connStates[id].state = Connected
		cm.afterEstablishConnection(c)
	} else {
		cm.lock.Lock()
		defer cm.lock.Unlock()
		_ = stream.Close()
		delete(cm.connStates, id)
	}
}

type WrappedMessage struct {
	Remote DeviceId
	Msg    interface{}
}

func (cm *ConnectionNode) afterEstablishConnection(c *Connection) {
	go func() {
		p := newParser(c.s)
		p.parse()
		for {
			select {
			case e, ok := <-p.errs:
				if !ok {
					return
				}
				log.Printf("%s when receive data \n", e.Error())
				if cm.ConnectionState(c.remote) == Connected {
					c.lock.Lock()
					c.err = WrappedError{
						occasion: ReadOccasion,
						source:   e,
					}
					cm.disConnectionReal(c.remote)
					c.lock.Unlock()
				} else {
					return
				}

			case msg, ok := <-p.msgs:
				if !ok {
					return
				}

				cm.msgs <- WrappedMessage{
					Msg:    msg,
					Remote: c.remote,
				}
			}
		}
	}()
}
