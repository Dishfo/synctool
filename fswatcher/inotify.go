package fswatcher

import (
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"
)

var (
	ErrNotAFolder = errors.New("It's not a folder")
)

var (
	IgnoredHide = true
)

//用于观察一个文件夹 以及文件夹下的子文件和子文件夹
type FolderWatcher struct {
	ep      *fdPoller
	wd      int
	root    string
	subDirs map[string]string

	paths map[int]string

	events chan Event
	errors chan error

	done     chan int
	doneResp chan int

	lock  sync.RWMutex
	subWd map[string]int
}

const (
	defaultMask = unix.IN_MOVED_TO | unix.IN_MOVED_FROM |
		unix.IN_CREATE | unix.IN_MODIFY |
		unix.IN_MOVE_SELF | unix.IN_DELETE | unix.IN_DELETE_SELF
)

//用于观察一个 folder
func NewWatcher(folder string) (*FolderWatcher, error) {
	folder = filepath.Clean(folder)
	w := new(FolderWatcher)

	w.subDirs = make(map[string]string)

	w.paths = make(map[int]string)

	w.events = make(chan Event, 1024*1024)
	w.errors = make(chan error, 256)
	w.subWd = make(map[string]int)

	w.done = make(chan int)
	w.doneResp = make(chan int)

	info, err := os.Stat(folder)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, ErrNotAFolder
	}

	w.root = folder
	w.wd, err = unix.InotifyInit1(unix.IN_NONBLOCK)
	if err != nil {
		return nil, err
	}

	err = w.addWatcher(folder)
	if err != nil {
		return nil, err
	}
	folders := findSubFolder(folder)

	for _, f := range folders {
		w.subDirs[f] = f

		err = w.addWatcher(f)

		if err != nil {
			return nil, err
		}
	}
	w.ep, err = newPoller(w.wd)
	if err != nil {
		return nil, err
	}
	go w.readEvents()

	return w, nil
}

//找出所有的子文件夹
func findSubFolder(folder string) []string {
	folders := []string{}
	infos, err := ioutil.ReadDir(folder)
	if err != nil {
		return folders
	}

	for _, info := range infos {
		if info.IsDir() {
			name := folder + "/" + info.Name()
			folders = append(folders, name)
			folders = append(folders, findSubFolder(name)...)
		}
	}

	return folders
}

func (w *FolderWatcher) Events() chan Event {
	return w.events
}

func (w *FolderWatcher) Errors() chan error {
	return w.errors
}

func (w *FolderWatcher) readEvents() {

	var (
		err error
		buf [unix.SizeofInotifyEvent * 4096]byte
		n   int
		ok  bool
	)

	defer close(w.doneResp)
	defer close(w.events)
	defer close(w.errors)
	defer unix.Close(w.wd)
	defer w.ep.close()
	for {
		if w.isClose() {
			log.Println("exit")
			return
		}
		ok, err = w.ep.wait()
		if err != nil {
			select {
			case w.errors <- err:
			case <-w.done:
				return
			}
			continue
		}

		if !ok {
			continue
		}

		n, err = unix.Read(w.wd, buf[:])
		if err == unix.EINTR {
			continue
		}

		if n < unix.SizeofInotifyEvent {
			err = errors.New("bytes read is not enough ")
			select {
			case w.errors <- err:
			case <-w.done:
				return
			}
			continue
		}

		if n < unix.SizeofInotifyEvent {
			var errno error
			if n == 0 {
				errno = io.EOF
			} else if n < 0 {
				errno = err
			}

			select {
			case w.errors <- errno:
			case <-w.done:
				return
			}

			continue
		}

		var offset uint32
		for offset <= uint32(n-unix.SizeofInotifyEvent) {
			raw := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))

			mask := uint32(raw.Mask)
			nameLen := uint32(raw.Len)
			name := w.paths[int(raw.Wd)]

			if nameLen > 0 {
				nameBytes := buf[offset+unix.SizeofInotifyEvent : offset+unix.SizeofInotifyEvent+nameLen]
				name += "/" + strings.TrimRight(string(nameBytes), "\000")
			}
			w.wrapEvent(mask, name, raw.Wd)
			offset += nameLen + unix.SizeofInotifyEvent
		}
	}
}

//包装event 并放入　event 信道
func (w *FolderWatcher) wrapEvent(mask uint32, name string, wd int32) {

	if shouldIgnoreEvent(name) {
		return
	}
	//todo 考虑移除阻塞操作
	/*	if w.isBlock(name) {
		discard = true
	}*/

	if mask&unix.IN_MOVED_TO == unix.IN_MOVED_TO ||
		mask&unix.IN_CREATE == unix.IN_CREATE {
		if w.handleCreate(mask, name) != nil {
			return
		}
	}

	if mask&unix.IN_DELETE_SELF == unix.IN_DELETE_SELF ||
		mask&unix.IN_MOVE_SELF == unix.IN_MOVE_SELF {
		delete(w.paths, int(wd))
		delete(w.subDirs, name)
		delete(w.subWd, name)
	}

	e := newEvent(mask, name)

	if e.Op != IGNORED {
		select {
		case w.events <- e:
		case <-w.done:
			return
		}
	}
}

func newEvent(mask uint32, name string) Event {
	var e = Event{
		Name: name,
	}

	if mask&unix.IN_CREATE == unix.IN_CREATE {
		e.Op |= CREATE
	}
	if mask&unix.IN_DELETE == unix.IN_DELETE {
		e.Op |= REMOVE
	}
	if mask&unix.IN_MODIFY == unix.IN_MODIFY {
		e.Op |= WRITE
	}
	if mask&unix.IN_MOVED_FROM == unix.IN_MOVED_FROM {
		e.Op |= MOVE
	}

	if mask&unix.IN_MOVED_TO == unix.IN_MOVED_TO {
		e.Op |= MOVETO
	}

	if mask&unix.IN_DELETE_SELF == unix.IN_DELETE_SELF ||
		mask&unix.IN_MOVE_SELF == unix.IN_MOVE_SELF ||
		mask&unix.IN_IGNORED == unix.IN_IGNORED {
		e.Op = IGNORED
	}

	return e
}

//递归得他添加文件夹的watcher
func (w *FolderWatcher) handleCreate(mask uint32, name string) error {
	if w.isClose() {
		return nil
	}
	info, err := os.Stat(name)
	if err != nil {
		return err
	}

	if info.IsDir() {
		err = w.addWatcher(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func shouldIgnoreEvent(file string) bool {

	if !IgnoredHide {
		return false
	}

	name := filepath.Base(file)
	if strings.HasPrefix(name, ".") {
		return true
	}
	return false
}

func (w *FolderWatcher) HasSubFolder(folder string) bool {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.subWd[folder] != 0
}

func (w *FolderWatcher) AppendWatcher(folder string) {
	err := w.addWatcher(folder)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("add ")
}

func (w *FolderWatcher) addWatcher(folder string) error {
	if w.isClose() {
		return nil
	}
	w.lock.Lock()
	wd, err := unix.InotifyAddWatch(w.wd, folder, defaultMask)
	if err != nil {
		return err
	}
	w.subWd[folder] = wd
	w.paths[wd] = folder
	w.lock.Unlock()
	return nil
}

func (w *FolderWatcher) Close() {
	if w.isClose() {
		return
	}
	_ = w.ep.wake()
	close(w.done)
	<-w.doneResp

}

func (w *FolderWatcher) isClose() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

func (w *FolderWatcher) BlockEvent(file string) {

}

func (w *FolderWatcher) UnBlockEvent(file string) {

}

func (w *FolderWatcher) isBlock(file string) bool {

	return false
}
