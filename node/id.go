package node

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"github.com/dchest/siphash"
	"github.com/segmentio/ksuid"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

func RandomID(prefix string) string {
	id := ksuid.New()
	return prefix + id.String()
}

/**
使用一个可用网卡的mac 地址 后面跟上时间戳
再通过 hash 算法生成一个64位散列值
 */

type DeviceId uint64

func NewUnqueId() (DeviceId, error) {
	var buf = make([]byte, 8)
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return 0, err
	}

	if len(netInterfaces) == 0 {
		return 0, errors.New("can't get  available net interfaces")
	}

	mac := netInterfaces[0].HardwareAddr
	now := time.Now().Nanosecond()
	binary.BigEndian.PutUint64(buf, uint64(now))
	mac = append(mac, buf...)

	source := rand.NewSource(time.Now().Unix())

	devid:=siphash.Hash(uint64(source.Int63()), uint64(source.Int63()), mac)

	return DeviceId(devid), nil
}

func (id DeviceId) Int64Value() int64 {
	return int64(id)
}
/**
使用base64来表示
 */
func (id DeviceId) String() string {
	data:=id.Bytes()
	return base64.StdEncoding.EncodeToString(data)
}

func (id DeviceId) Bytes() []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(id))
	return bytes
}

//requestID generator

type RequestIdGernator int32

func NewReqIdGnernator ()RequestIdGernator{
	return 0
}

func (g *RequestIdGernator)Next() int32{
	return atomic.AddInt32((*int32)(g),1)
}


func GenerateIdFromString(id string) (DeviceId,error){
	idbytes,err:=base64.StdEncoding.DecodeString(id)
	if err!=nil{
		return 0,err
	}

	devid:=binary.BigEndian.Uint64(idbytes)
	return DeviceId(devid),nil
}

func GenerateIdFromBytes(data []byte) (DeviceId){
	devid:=binary.BigEndian.Uint64(data)
	return DeviceId(devid)
}
