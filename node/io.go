package node

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-net"
	"log"
	"syncfolders/bep"
)

/**
提供一系列的 msg byte 转化操作
使用缓存的byte池进行msg的发送
对于非法的数据构成同样应该进行切断
*/
const CompressThresHold = 1024 * 1024
const MagicNumber int32 = 0x2EA7D90B

var (
	ErrInvaildMagic  = errors.New("Invaild magic number ")
	ErrWrongDataSize = errors.New("Wrong data array size ")
	ErrInvaildMsg    = errors.New("a invaild message")
	ErrNotEough      = errors.New("has not receive enough data")
	ErrShortSlice    = errors.New("use a short slice ")
)

func sendHello(s net.Stream, hello *bep.Hello) error {
	byteSize := 4 + 2 + hello.XXX_Size()
	b := GetByteArray(byteSize)
	buf := bytes.NewBuffer(b)
	err := binary.Write(buf, binary.BigEndian, MagicNumber)
	if err != nil {
		log.Panicf("%s when send Hello ", err.Error())
	}

	err = binary.Write(buf, binary.BigEndian, int16(hello.XXX_Size()))
	if err != nil {
		log.Panicf("%s when send Hello ", err.Error())
	}

	data, err := proto.Marshal(hello)
	if err != nil {
		panic(err)
	}

	buf.Write(data)

	if err != nil {
		panic(err)
	}
	_, err = s.Write(buf.Bytes())
	log.Printf("write hello %p %s", hello, s.Conn().RemotePeer().Pretty())
	return err
}

func receiveHello(s net.Stream) (*bep.Hello, error) {
	var magic int32
	var datalen int16
	hello := new(bep.Hello)

	err := binary.Read(s, binary.BigEndian, &magic)
	if err != nil {
		return nil, err
	}
	if magic != MagicNumber {
		return nil, ErrInvaildMagic
	}

	err = binary.Read(s, binary.BigEndian, &datalen)
	if err != nil {
		return nil, err
	}

	data := make([]byte, datalen, datalen)
	err = readNByte(s, data, int(datalen))
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(data, hello)
	if err != nil {
		return nil, err
	}
	log.Printf("read hello %p %s", hello, s.Conn().RemotePeer().Pretty())
	return hello, nil
}

//todo 介入lz4
//把msg 转化为 bytes ,可能返回 表示非法msg的错误
func wrapMessage(msg interface{}) ([]byte, error) {
	var data []byte
	var err error
	var headerdata []byte
	header := new(bep.Header)

	switch msg.(type) {
	case *bep.Index:
		header.Type = bep.MessageType_INDEX
	case *bep.IndexUpdate:
		header.Type = bep.MessageType_INDEX_UPDATE
	case *bep.Request:
		header.Type = bep.MessageType_REQUEST
	case *bep.Response:
		header.Type = bep.MessageType_RESPONSE
	case *bep.Ping:
		header.Type = bep.MessageType_PING
	case *bep.Close:
		header.Type = bep.MessageType_CLOSE
	case *bep.ClusterConfig:
		header.Type = bep.MessageType_CLUSTER_CONFIG
	default:
		return nil, ErrInvaildMsg
	}

	if pmsg, ok := msg.(proto.Message); !ok {
		return nil, ErrInvaildMsg
	} else {
		data, err = proto.Marshal(pmsg)
		if err != nil {
			return nil, ErrInvaildMsg
		}
	}

	if len(data) > CompressThresHold {
		header.Compression = bep.MessageCompression_LZ4
		data, err = lz4Encode(data)
		if err != nil {
			return nil, err
		}
	} else {
		header.Compression = bep.MessageCompression_NONE
	}

	b := GetByteArray(len(data) + header.XXX_Size() + 4 + 2)
	buf := bytes.NewBuffer(b)
	err = binary.Write(buf, binary.BigEndian, int16(header.XXX_Size()))
	if err != nil {
		panic(err)
	}

	headerdata, err = proto.Marshal(header)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(headerdata)
	if err != nil {
		panic(err)
	}

	err = binary.Write(buf, binary.BigEndian, int32(len(data)))
	if err != nil {
		panic(err)
	}

	_, err = buf.Write(data)
	if err != nil {
		panic(err)
	}

	return buf.Bytes(), nil
}

func GetByteArray(size int) []byte {
	return make([]byte, 0, size)
}

//readNByte 从stream 中读取若干字节或者是字节数目不够都以错误处理
//主要使用解析时使用
func readNByte(s net.Stream, p []byte, n int) error {
	var (
		readn = 0
	)

	if n < 0 {
		return errors.New("invaild length")
	}

	if len(p) < n {
		return ErrShortSlice
	}
	for readn < n {
		n, err := s.Read(p[readn:])
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		readn += n
	}
	if readn != n {
		return ErrNotEough
	}
	return nil
}
