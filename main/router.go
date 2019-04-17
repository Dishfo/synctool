package main

import "net/http"

/**
设置http handler
 */
func route()  {
	http.HandleFunc("/nodeId",nodeId)
	http.HandleFunc("/updates",getUpdates)

	http.HandleFunc("/addFolder",AddFolder)
	http.HandleFunc("/addDevice",AddDevice)

	http.HandleFunc("/devices",DeviceInfos)
	http.HandleFunc("/folders",FolderInfos)
}
