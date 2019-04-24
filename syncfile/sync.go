package syncfile

import (
	"github.com/libp2p/go-libp2p-peer"
	"sync"
	"syncfolders/bep"
	"syncfolders/fs"
	"syncfolders/node"
	"time"
)

var (
	reqIds = new(int64)
)

/**
用于	描述同步后的文件内容
*/

type TargetFile struct {
	Folder string
	Name   string
	Dst    *bep.FileInfo
	Blocks []*FileBlock
}

type FileBlock struct {
	From   node.DeviceId
	Folder string
	Name   string
	Offset int64
	Size   int32
	Hash   []byte
}

type TargetFiles struct {
	Folders []*TargetFile
	Links   []*TargetFile
	Files   []*TargetFile
}

type SyncHandler interface {
	Handle(files TargetFiles, resp []*bep.Response, iset IntSet)
}

/**
提交	任务后 返回一个 任务 id ,
*/

type SyncManager struct {
	fsys        *fs.FileSystem
	cn          *node.ConnectionNode
	connectFlag map[node.DeviceId]bool
	folders     map[string]*ShareFolder
	lock        sync.RWMutex
	devLock     sync.RWMutex
	folderLock  sync.RWMutex
	tm          *TaskManager

	hostIds map[node.DeviceId]peer.ID
	devices map[node.DeviceId]*bep.Device

	rwm *requestWaitingManager

	inSendUpdateTranscation bool
	inConnectionTranscation bool
	//再同步事务中表示处理过的最后一个 update 对应的存储id
	reqIdGenerator *int64
}

type ReceiveIndex struct {
	remote    node.DeviceId
	timestamp int64
	index     *bep.Index
}

type ReceiveIndexUpdate struct {
	Id        int64
	remote    node.DeviceId
	timestamp int64
	update    *bep.IndexUpdate
}

func NewSyncManager(fsys *fs.FileSystem,
	cn *node.ConnectionNode) *SyncManager {
	sm := new(SyncManager)
	sm.cn = cn
	sm.fsys = fsys

	sm.connectFlag = make(map[node.DeviceId]bool)
	sm.folders = make(map[string]*ShareFolder)

	sm.tm = NewTaskManager()
	sm.reqIdGenerator = new(int64)

	sm.hostIds = make(map[node.DeviceId]peer.ID)
	sm.devices = make(map[node.DeviceId]*bep.Device)
	sm.rwm = newReqWaitManager(sm)
	sm.setupTimerTask()

	return sm
}

func (sm *SyncManager) ProvideNotification(remote node.DeviceId) *node.ConnectionNotification {
	notify, err := sm.cn.RegisterNotification(remote)
	if err != nil {
		return nil
	}
	return notify
}

/**
基于稳定的连接逻辑
与update交换功能实现的部分
*/

func (sm *SyncManager) setupTimerTask() {
	sm.tm.AddTask(Task{
		Dur:  int64(time.Second * 8),
		Type: TASK_DUR,
		Act: func() {
			sm.preparedConnect()
		},
	})

	sm.tm.AddTask(Task{
		Dur:  int64(time.Second * 5),
		Type: TASK_DUR,
		Act: func() {
			sm.prepareSendUpdate()
		},
	})

	go sm.receiveWorker()

	sm.tm.AddTask(Task{
		Dur:  int64(time.Second * 10),
		Type: TASK_DUR,
		Act: func() {
			sm.prepareSync()
		},
	})
}
