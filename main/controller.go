package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
	"syncfolders/syncfile"
)

/**
用于提供http handler
*/
//node io 通信模块出现问题
var (
	cn                 *node.ConnectionNode = nil
	fsys               *fs.FileSystem
	sm                 *syncfile.SyncManager
	defaultSyncVersion = "sync1.0.0"
	defaultClientName  = "sync"
)

type H struct {
}

func (h H) ConfirmHello(hello *bep.Hello) bool {
	if hello.ClientVersion != defaultSyncVersion {
		return false
	} else if hello.ClientName != defaultClientName {
		return false
	}
	return true
}

func initNode() {
	var err error
	cn, err = node.NewConnectionNode(configs)
	if err != nil {
		log.Fatal(err)
	}
	node.ClientName = defaultClientName
	node.ClientVersion = defaultSyncVersion
	name, _ := cn.Ids()
	node.DeviceName = name
	node.RegisterHandShake(H{})

}

func initFs() {
	fsys = fs.NewFileSystem()
}

func initSync() {
	sm = syncfile.NewSyncManager(fsys, cn)
	fs.SetLocalId(sm.LocalId())
}

func nodeId(w http.ResponseWriter, r *http.Request) {
	dId, hostId := cn.Ids()
	m := map[string]string{
		"Device": dId,
		"P2P":    hostId,
	}

	data, err := json.MarshalIndent(&m, "", " ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}

}

const (
	FolderIdTag   = "folderId"
	FolderPathTag = "folderPath"
)

//测试用接口
func addFolder(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folderId := r.Form.Get(FolderIdTag)
	folderpath := r.Form.Get(FolderPathTag)
	log.Println(folderId, folderpath)
	if folderId == "" || folderpath == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err := fsys.AddFolder(folderId, folderpath)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

//getUpdates 用于测试阶段的函数，并不是一个正式的接口
func getUpdates(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folderId := r.Form.Get(FolderIdTag)
	if folderId == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	updates := fsys.GetUpdates(folderId)
	for _, u := range updates {
		_, _ = w.Write([]byte(
			u.String()))
	}
}

func AddFolder(w http.ResponseWriter, r *http.Request) {
	opt := new(syncfile.FolderOption)
	_ = r.ParseForm()
	err := parseFolderOpt(r.Form, opt)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	err = sm.AddFolder(opt)

	if err != nil {
		log.Printf("%s when add folder", err.Error())
		_, _ = w.Write([]byte(err.Error()))
	}
}

const (
	HostIdTag = "HostId"
)

func AddDevice(w http.ResponseWriter, r *http.Request) {
	device := new(bep.Device)
	_ = r.ParseForm()
	err := parseDevice(r.Form, device)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	hostId := r.Form.Get(HostIdTag)
	sm.NewDevice(device, hostId)
}

func DeviceInfos(w http.ResponseWriter, r *http.Request) {
	devices := sm.Devices()
	for _, device := range devices {
		p, err := json.MarshalIndent(device, "", " ")
		if err != nil {
			continue
		}
		_, _ = w.Write(p)
		_, _ = w.Write([]byte("\r\n"))
	}
}

func FolderInfos(w http.ResponseWriter, r *http.Request) {
	folders := sm.GetFolders()
	for _, folder := range folders {
		p, err := json.MarshalIndent(folder, "", " ")
		if err != nil {
			continue
		}
		_, _ = w.Write(p)
		_, _ = w.Write([]byte("\r\n"))
	}
}

func getIndexData(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folderId := r.PostForm.Get("FolderId")
	index := fsys.GetIndex(folderId)
	data, err := json.MarshalIndent(index, "", " ")
	if err == nil {
		_, _ = w.Write(data)
	} else {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func getFileData(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folder := r.PostForm.Get("Folder")
	name := r.PostForm.Get("Name")
	offSetStr := r.PostForm.Get("Offset")
	SizeStr := r.PostForm.Get("Size")

	tmp, _ := strconv.ParseInt(offSetStr, 10, 64)
	offset := tmp
	tmp, _ = strconv.ParseInt(SizeStr, 10, 32)
	size := int32(tmp)
	data, err := fsys.GetData(folder, name, offset, size)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
	} else {
		_, _ = w.Write(data)

	}
}

func getIndexSeq(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folderId := r.PostForm.Get("FolderId")
	seqStr := r.PostForm.Get("seq")
	seq, _ := strconv.ParseInt(seqStr, 10, 64)
	tx, err := fs.GetTx()
	if err != nil {
		panic(err)
	}
	defer tx.Commit()
	indexSeqs, err := fs.GetIndexSeqAfter(tx, int64(seq), folderId)
	if err != nil {
		panic(err)
	} else {
		data, err := json.MarshalIndent(indexSeqs, "", "  ")
		if err == nil {
			_, _ = w.Write(data)
		} else {
			_, _ = w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func getUpdateData(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	folderId := r.PostForm.Get("FolderId")
	updates := fsys.GetUpdates(folderId)
	data, err := json.MarshalIndent(updates, "", " ")
	if err == nil {
		_, _ = w.Write(data)
	} else {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func parseFolderOpt(data url.Values,
	opt *syncfile.FolderOption) error {
	var err error
	log.Println(data)
	opt.Id = data.Get("Id")
	opt.ReadOnly, err = strconv.ParseBool(data.Get("ReadOnly"))
	if err != nil {
		return err
	}
	opt.Label = data.Get("Label")
	opt.Real = data.Get("Real")
	deviceStr := data.Get("Devices")
	devices := strings.Split(deviceStr, ",")
	opt.Devices = make([]string, 0)
	for _, device := range devices {
		if device == "" {
			continue
		}
		opt.Devices = append(opt.Devices, device)
	}

	log.Println(deviceStr, "devices is ")
	return nil
}

func parseDevice(data url.Values,
	dev *bep.Device) error {
	dev.Name = data.Get("Name")
	id := data.Get("Id")
	devId, err := node.GenerateIdFromString(id)
	if err != nil {
		return err
	}
	dev.Id = devId.Bytes()
	return nil
}
