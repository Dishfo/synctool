package node

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-net"
	"sync"
	"syncfolders/bep"
)

/**
对于	每一个stream  开启一个go rountine
不断	的接收数据 ，把字节数据转化为 bep msg
保证	读行为发生在一个 线程上
*/

/**
数据的解析者
出现错误时会导致这个解析者的关闭
解析有一下几个步骤
read header length
read header
unmashal header
read message length
read message
unmashal message
严格的处理过程
*/

const (
	HeaderLenCap  = 2
	MessageLenCap = 4
)

var (
	ErrInvaildMessage = errors.New("receive message data is invaild ")
)

type messageParser struct {
	s      net.Stream
	lock   sync.Mutex
	closed bool
	errs   chan error
	msgs   chan interface{}

	//两个较长生命周期的slice 用于接收header 长度和msg 的长度
	headerlenb []byte
	msglenb    []byte
}

func newParser(s net.Stream) *messageParser {
	parser := new(messageParser)
	parser.s = s
	parser.errs = make(chan error)
	parser.msgs = make(chan interface{}, 30)

	parser.headerlenb = make([]byte, HeaderLenCap, HeaderLenCap)
	parser.msglenb = make([]byte, MessageLenCap, MessageLenCap)

	return parser
}

func (p *messageParser) IsClose() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.closed
}

func (p *messageParser) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return
	}
	close(p.errs)
	close(p.msgs)
	p.closed = true
}

func (p *messageParser) parse() {
	go func() {
		for !p.IsClose() {
			header, err := p.readHeader()
			if err != nil {
				p.errs <- err
				p.Close()
				break
			}

			msg, err := p.readMessage(header)
			if err != nil {
				p.errs <- err
				p.Close()
				break
			}
			p.msgs <- msg
		}
	}()
}

func (p *messageParser) readHeader() (*bep.Header, error) {
	err := readNByte(p.s, p.headerlenb, HeaderLenCap)
	if err != nil {
		return nil, err
	}
	headerLen := int(binary.BigEndian.Uint16(p.headerlenb))
	b := make([]byte, headerLen, headerLen)
	err = readNByte(p.s, b, headerLen)
	if err != nil {
		return nil, err
	}
	header := new(bep.Header)
	err = proto.Unmarshal(b, header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (p *messageParser) readMessage(header *bep.Header) (interface{}, error) {
	err := readNByte(p.s, p.msglenb, MessageLenCap)
	if err != nil {
		return nil, err
	}

	msglen := int(binary.BigEndian.Uint32(p.msglenb))
	b := make([]byte, msglen, msglen)

	err = readNByte(p.s, b, msglen)
	if err != nil {
		return nil, err
	}

	if header.Compression == bep.MessageCompression_LZ4 {
		b, err = lz4Decode(b)
		if err != nil {
			return nil, err
		}
	}

	return unmarshalMsg(b, header.Type)
}

func unmarshalMsg(p []byte, t bep.MessageType) (interface{}, error) {
	switch t {
	case bep.MessageType_CLUSTER_CONFIG:
		config := new(bep.ClusterConfig)
		err := proto.Unmarshal(p, config)
		return config, err
	case bep.MessageType_INDEX:
		index := new(bep.Index)
		err := proto.Unmarshal(p, index)
		return index, err
	case bep.MessageType_INDEX_UPDATE:
		update := new(bep.IndexUpdate)
		err := proto.Unmarshal(p, update)
		return update, err
	case bep.MessageType_RESPONSE:
		resp := new(bep.Response)
		err := proto.Unmarshal(p, resp)
		return resp, err
	case bep.MessageType_REQUEST:
		req := new(bep.Request)
		err := proto.Unmarshal(p, req)
		return req, err
	case bep.MessageType_PING:
		ping := new(bep.Request)
		err := proto.Unmarshal(p, ping)
		return ping, err
	case bep.MessageType_CLOSE:
		c := new(bep.Close)
		err := proto.Unmarshal(p, c)
		return c, err
	default:
		return nil, ErrInvaildMsg
	}
}
