package syncfile

import (
	"errors"
	"github.com/libp2p/go-libp2p-peer"
	"log"
	"sync"
	"syncfolders/bep"
	"syncfolders/node"
)

/**
同步模块下的设备管
*/

func (sm *SyncManager) NewDevice(dev *bep.Device, hostId string) {
	pId, err := peer.IDB58Decode(hostId)
	if err != nil {
		log.Printf("%s when add a new device ", err.Error())
		return
	}

	id := idOfDevice(dev)
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	sm.hostIds[id] = pId

	dst := new(bep.Device)
	copyDevice(dst, dev)
	sm.devices[id] = dst
}

//addDevice when know a info about a device then add this device into devices
func (sm *SyncManager) addDevice(dev *bep.Device) {
	dst := new(bep.Device)
	copyDevice(dst, dev)
	id := idOfDevice(dev)
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	if _, ok := sm.devices[id]; !ok {
		sm.devices[id] = dst
	}
}

func (sm *SyncManager) RemoveDeice(device string) {
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	devId, err := node.GenerateIdFromString(device)
	if err != nil {
		return
	}
	delete(sm.devices, devId)
	sm.DisConnection(devId)
	delete(sm.hostIds, devId)
}

//with lock
func (sm *SyncManager) Devices() []*bep.Device {
	devices := make([]*bep.Device, 0)
	sm.devLock.RLock()
	defer sm.devLock.RUnlock()
	for _, dev := range sm.devices {
		dst := new(bep.Device)
		copyDevice(dst, dev)
		devices = append(devices, dst)
	}
	return devices
}

func (sm *SyncManager) Device(id node.DeviceId) *bep.Device {
	sm.devLock.RLock()
	defer sm.devLock.RUnlock()
	dst := new(bep.Device)
	if dev, ok := sm.devices[id]; ok {
		copyDevice(dst, dev)
		return dst
	} else {
		dst.Id = id.Bytes()
		return dst
	}
}

func idOfDevice(dev *bep.Device) node.DeviceId {
	id := node.GenerateIdFromBytes(dev.Id)
	return id
}

func copyDevice(dst, src *bep.Device) {
	dst.Name = src.Name
	dst.Id = make([]byte, len(src.Id))
	copy(dst.Id, src.Id)
	dst.Addresses = make([]string, len(src.Addresses))
	copy(dst.Addresses, src.Addresses)
}

func (sm *SyncManager) onConnected(id node.DeviceId) {
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	sm.connectFlag[id] = true
}

func (sm *SyncManager) DisConnection(id node.DeviceId) {
	sm.onDisConnected(id)
	sm.cn.DisConnect(id)
}

//with lock
func (sm *SyncManager) onDisConnected(id node.DeviceId) {
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	sm.connectFlag[id] = false
}

func (sm *SyncManager) preparedConnect() {
	if !sm.startConnectionTransacation() {
		return
	}

	sm.devLock.Lock()
	backups := make(map[node.DeviceId]peer.ID)
	for k, v := range sm.hostIds {
		backups[k] = v
	}

	sm.devLock.Unlock()
	var wg sync.WaitGroup
	for k := range backups {

		if sm.cn.ConnectionState(k) != node.Connected {
			sm.DisConnection(k)
		}

		if sm.isConnectd(k) {
			continue
		}

		wg.Add(1)
		id := k
		go func() {
			err := sm.connectDevice(id)
			if err == nil {
				sm.hasConnected(id)
			}
			if err == node.ErrHasConnected {
				if !sm.isConnectd(id) {
					sm.hasConnected(id)
				}
			}
			if err != nil {
				log.Printf("%s when connection %s", err.Error(),
					id.String())
			}
			wg.Done()
		}()
	}
	wg.Wait()
	sm.endConnectionTransacation()
}

//表示sm进入设备连接的逻辑处理
func (sm *SyncManager) startConnectionTransacation() bool {
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	if sm.inConnectionTranscation {
		return false
	}
	sm.inConnectionTranscation = true
	return true
}

func (sm *SyncManager) endConnectionTransacation() {
	sm.devLock.Lock()
	defer sm.devLock.Unlock()
	sm.inConnectionTranscation = false
}

//hasConnected 成功的建立连接后调用,设置连接标记,
func (sm *SyncManager) hasConnected(id node.DeviceId) {
	addrs := sm.cn.DeviceAddrs(id)

	sm.devLock.Lock()
	dev := sm.devices[id]
	sm.devLock.Unlock()

	if dev == nil {
		panic("lost a ptr of device ")
	}
	dev.Addresses = make([]string, len(addrs))
	copy(dev.Addresses, addrs)
	configs := sm.generateClusterConfig()

	err := sm.SendMessage(id, configs)
	if err != nil {
		log.Printf("%s when establish connction with %s",
			err.Error(), id.String())
		sm.cn.DisConnect(id)
		return
	} else {
		sm.devLock.Lock()
		sm.connectFlag[id] = true
		log.Println("succeed connection")
		sm.devLock.Unlock()
	}
}

//with lock
func (sm *SyncManager) connectDevice(id node.DeviceId) error {
	sm.devLock.RLock()
	if pid, ok := sm.hostIds[id]; ok {
		sm.devLock.RUnlock()
		return sm.cn.ConnectDevice(id, pid.Pretty())
	} else {
		sm.devLock.RUnlock()
		return errors.New("unkonw host id of device ")
	}
}

func (sm *SyncManager) isConnectd(id node.DeviceId) bool {
	sm.devLock.RLock()
	defer sm.devLock.RUnlock()
	return sm.connectFlag[id]
}

//generateClusterConfig 生成当前状态下的lcusterConfig
func (sm *SyncManager) generateClusterConfig() *bep.ClusterConfig {
	config := new(bep.ClusterConfig)
	config.Folders = make([]*bep.Folder, 0)
	folders := sm.GetFolders()
	for _, folder := range folders {
		bepFolder := new(bep.Folder)
		bepFolder.Id = folder.Id
		bepFolder.ReadOnly = folder.ReadOnly
		bepFolder.Label = folder.Label
		bepFolder.Paused = false //这个field 是一个未实现的功能
		bepFolder.Devices = make([]*bep.Device, 0)
		for _, dev := range folder.Devices {
			id, _ := node.GenerateIdFromString(dev)
			device := sm.Device(id)
			bepFolder.Devices = append(bepFolder.Devices, device)
		}
		config.Folders = append(config.Folders, bepFolder)
	}
	return config
}

/**getConnectedDevice return has been connected devices
this method may return a has DisConnected device
*/
func (sm *SyncManager) getConnectedDevice() []node.DeviceId {
	sm.devLock.RLock()
	defer sm.devLock.RUnlock()
	devIds := make([]node.DeviceId, 0)
	for k, v := range sm.connectFlag {
		if v {
			devIds = append(devIds, k)
		}
	}
	return devIds
}
