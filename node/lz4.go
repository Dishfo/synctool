package node

import (
	"bytes"
	"github.com/pierrec/lz4"
	"io/ioutil"
)

func lz4Encode(data []byte)([]byte, error) {
	var(
		err error
	)
	buf:=bytes.NewBuffer(nil)
	lw:=lz4.NewWriter(buf)
	_,err=lw.Write(data)
	if err!=nil{
		return nil,err
	}
	_=lw.Flush()
	return buf.Bytes(), nil
}


func lz4Decode(data []byte) ([]byte, error) {
	var(
		err error
	)
	buf:=bytes.NewBuffer(data)
	lr:=lz4.NewReader(buf)
	uncompressed,err:=ioutil.ReadAll(lr)
	return uncompressed, err
}









































