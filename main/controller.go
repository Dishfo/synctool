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

var (
	cn   *node.ConnectionNode = nil
	fsys *fs.FileSystem
	sm   *syncfile.SyncManager
)

func initNode() {
	var err error
	cn, err = node.NewConnectionNode(configs)
	if err != nil {
		log.Fatal(err)
	}
}

func initFs() {
	fsys = fs.NewFileSystem()
}

func intiSync() {
	sm = syncfile.NewSyncManager(fsys, cn)
}

func nodeId(w http.ResponseWriter, r *http.Request) {
	dId, hostId := cn.Ids()
	m := map[string]string{
		"Device": dId,
		"P2P":    hostId,
	}

	data, err := json.Marshal(&m)
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
	//folders := sm.GetFolders()
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

func parseFolderOpt(data url.Values,
	opt *syncfile.FolderOption) error {
	var err error
	opt.Id = data.Get("Id")
	opt.ReadOnly, err = strconv.ParseBool(data.Get("ReadOnly"))
	if err != nil {
		return err
	}
	opt.Label = data.Get("Label")
	opt.Real = data.Get("Real")
	deviceStr := data.Get("Devices")
	opt.Devices = strings.Split(deviceStr, ",")
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
