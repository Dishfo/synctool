package main

import "net/http"

/**
设置http handler
*/
func route() {
	http.HandleFunc("/nodeId", nodeId)
	http.HandleFunc("/updates", getUpdates)

	http.HandleFunc("/addFolder", AddFolder)
	http.HandleFunc("/addDevice", AddDevice)
	http.HandleFunc("/editFolder", EditFolder)
	http.HandleFunc("/removeFolder", RemoveFolder)
	http.HandleFunc("/removeDevice", RemoveDevice)

	http.HandleFunc("/devices", DeviceInfos)
	http.HandleFunc("/folders", FolderInfos)

	//test http point
	http.HandleFunc("/index", getIndexData)
	http.HandleFunc("/udpates", getUpdateData)
	http.HandleFunc("/indexSeq", getIndexSeq)

	http.HandleFunc("/fileblock", getFileData)

}
