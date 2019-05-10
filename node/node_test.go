package node

import (
	"bytes"
	"database/sql"
	"log"
	"syncfolders/bep"
	"testing"
)

var (
	bootStrapAddr = "/ip4/127.0.0.1/tcp/" +
		"8089/ipfs/QmbUJ9psXC4ZGxWBPa1c1jtW9kNqJEnXsZ71YdeMRgL3HV"
	listenAddr = "/ip4/127.0.0.1/tcp/9000"
)

func TestBaseNode_Init(t *testing.T) {
	configs := make(map[string]string)
	configs[TagBootStrap] = bootStrapAddr
	configs[TagListen] = listenAddr
	node, err := NewNode(configs)
	if err != nil {
		t.Fatal(err, " on Create Node")
	} else {
		t.Log("succeed")

		node.Close()
	}
}

func TestConnectionNode(t *testing.T) {
	configs := make(map[string]string)
	configs[TagBootStrap] = bootStrapAddr
	configs[TagListen] = listenAddr

	cn, err := NewConnectionNode(configs)
	if err != nil {
		t.Fatal(err, " on Create Node")
	} else {
		t.Log("succeed")
		cn.Close()
	}

}

func TestSliceCap(t *testing.T) {
	b := GetByteArray(20)
	log.Println(cap(b), len(b))
	b1 := b[:15]
	b2 := b[:20]
	b1[14] = 77
	log.Println(b2[14])
	buf := bytes.NewBuffer(b)
	buf.Write([]byte("123456789"))
	log.Println(b2)
}

type testHandler struct {
}

func (h *testHandler) ConfirmHello(hello *bep.Hello) bool {
	log.Println(*hello)
	return true
}

func TestConnectionNode_ConnectDevice(t *testing.T) {

	DeviceName = "synctool"
	ClientName = "hello"
	ClientVersion = "1.0.0"
	RegisterHandShake(&testHandler{})

	n1 := node1()
	n2 := node2()
	log.Println(n1.Ids())
	log.Println(n2.Ids())

	id1, h1 := n1.Ids()
	id2, h2 := n2.Ids()

	devId1, _ := GenerateIdFromString(id1)
	devId2, _ := GenerateIdFromString(id2)

	err := n1.ConnectDevice(devId2, h2)
	if err != nil {
		log.Println(err)
	}

	err = n2.ConnectDevice(devId1, h1)
	if err != nil {
		log.Println(err)
	}

	for {
		err = n2.ConnectDevice(devId1, h1)
		if err != nil {
			log.Println(err)
			if err == ErrHasConnected {
				break
			}
		}
	}

	ping := bep.Ping{}
	err = n1.SendMessage(devId2, &ping)
	index := new(bep.Index)
	index.Folder = "123456"
	index.Files = make([]*bep.FileInfo, 4, 4)
	for i, _ := range index.Files {
		f := new(bep.FileInfo)
		index.Files[i] = f
		f.Name = "awdawd"
	}
	err = n1.SendMessage(devId2, index)
	//err = n1.SendMessage(devId2, &ping)
	//err = n1.SendMessage(devId2, &ping)
	if err != nil {
		log.Println(err)
	} else {
		log.Println("send succeed ")
	}

	msgs := n2.Messages()

	select {
	case msg := <-msgs:
		log.Println(msg.Msg)
	}

	log.Println("has connected ")
}

func node1() *ConnectionNode {
	configs := make(map[string]string)
	configs[TagBootStrap] = bootStrapAddr
	configs[TagListen] = "/ip4/127.0.0.1/tcp/9001"
	node, err := NewConnectionNode(configs)
	if err != nil {
		panic(err)
	}

	return node
}

func node2() *ConnectionNode {
	configs := make(map[string]string)
	configs[TagBootStrap] = bootStrapAddr
	configs[TagListen] = "/ip4/127.0.0.1/tcp/9002"
	node, err := NewConnectionNode(configs)
	if err != nil {
		panic(err)
	}

	return node
}

func TestMemorySql(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatalln(err)
	}

	_ = db.Close()

}
