package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-host"
	"github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"
	"log"
)

/**
描述了一个节点 id 以及 libp2p 的host Id
具备节点之间的通信基础逻辑
*/


//todo 提供返回节点地址的函数
const (
	TagListen    = "NodeListen"
	TagBootStrap = "DiscoverServer"
)

var (
	ErrInvaildConfig = errors.New("InCompete Configureation")
)

type BaseNode struct {
	ctx     context.Context
	id      DeviceId
	h       host.Host
	dhtPeer *dht.IpfsDHT
	//close about
	cfun context.CancelFunc
}

//NewNode return new node or error
func NewNode(configs map[string]string) (*BaseNode, error) {
	var err error
	node := new(BaseNode)
	node.id,err = NewUnqueId()

	if err!=nil{
		return  nil,fmt.Errorf("%s on create id",err.Error())
	}

	listen, ok := configs[TagListen]
	if !ok {
		return nil, ErrInvaildConfig
	}

	boostrap, ok := configs[TagBootStrap]
	if !ok {
		return nil, ErrInvaildConfig
	}

	listenmaddr, err := multiaddr.NewMultiaddr(listen)
	if err != nil {
		return nil, ErrInvaildConfig
	}

	bootmaddr, err := multiaddr.NewMultiaddr(boostrap)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrs(listenmaddr),
	}

	ctx := context.Background()
	node.ctx = ctx
	child, cfunc := context.WithCancel(ctx)
	node.cfun = cfunc

	node.h, err = libp2p.New(child, opts...)
	if err != nil {
		return nil, err
	}

	dhtPeer, err := dht.New(child, node.h)
	if err!=nil {
		return nil,err
	}
	err = bootStrapDht(dhtPeer,node.h,bootmaddr,child)
	if err!=nil {
		return  nil,err
	}

	node.dhtPeer=dhtPeer
	log.Printf(" %s has run \n",node.id.String())
	//set connection Manager
	return node, nil
}

func bootStrapDht(dhtPeer *dht.IpfsDHT,
	h host.Host,
	bootmaddr multiaddr.Multiaddr,
	ctx context.Context) error {

	err := dhtPeer.Bootstrap(ctx)
	if err!= nil {
		return err
	}

	peerInfo,err := peerstore.InfoFromP2pAddr(bootmaddr)
	if err!=nil {
		return  err
	}

	err = h.Connect(ctx,*peerInfo)
	if err!=nil {
		return  err
	}

	return nil
}

func (n *BaseNode) Addr() []string {
	addrs := n.h.Addrs()
	addrstrs := make([]string,0)
	for _,addr := range  addrs {
		addrstrs= append(addrstrs, addr.String())
	}
	return addrstrs
}


func (n *BaseNode) Close() {
	n.cfun()
}


//Ids return deviceId of node and p2pId  of the node
func (n *BaseNode) Ids()(string,string) {
	return  n.id.String(),n.h.ID().Pretty()
}


