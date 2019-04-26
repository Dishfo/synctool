package watcher

import (
	"errors"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

/**
提供文件夹监视功能
*/
//todo add sub watcher
/**
todo
 建立额外的fsnotify.watcher
 当前的fsnotify 无法确定 rename产生的新文件名
*/
type FolderWatcher struct {
	folder   string
	watcher  *fsnotify.Watcher
	eventMux sync.Mutex
	isclose  bool
	evnets   chan fsnotify.Event
}

var (
	ErrWatcherhasset = errors.New("this watcher has been set folder ")
)

/**
NewWatcher will retuen a ptr of FileWatcher
*/
func NewWatcher() (*FolderWatcher, error) {
	var (
		err error = nil
	)
	w := new(FolderWatcher)
	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w.evnets = make(chan fsnotify.Event, 50)
	go func() {
		for {
			select {
			case e, ok := <-w.watcher.Events:
				if !ok {
					return
				} else {
					w.onEvent(e)
				}
			}
		}
	}()

	return w, err
}

//close fsnotify of folder
func (w *FolderWatcher) Close() {
	w.eventMux.Lock()
	defer w.eventMux.Unlock()
	_ = w.watcher.Close()
	w.isclose = true
}

//IsClose return true if has close
func (w *FolderWatcher) IsClose() bool {
	w.eventMux.Lock()
	defer w.eventMux.Unlock()
	return w.isclose
}

//为watcher设置一个folder
func (w *FolderWatcher) SetFolder(path string) error {
	var (
		err error = nil
	)

	if w.folder != "" {
		return ErrWatcherhasset
	}

	if err = w.watcher.Add(path); err == nil {
		w.folder, _ = filepath.Abs(path)
		err = interalAddFolder(w, w.folder)
		if err != nil {
			return err
		}
		go func() {

		}()
	}
	return err
}

func interalAddFolder(w *FolderWatcher, folder string) error {
	infos, err := ioutil.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, info := range infos {
		if info.IsDir() {
			subfolder := filepath.Join(folder, info.Name())
			err := w.watcher.Add(subfolder)
			if err != nil {
				return err
			}
			err = interalAddFolder(w, subfolder)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *FolderWatcher) onEvent(e fsnotify.Event) {
	//	log.Println(e)
	switch e.Op {
	case fsnotify.Remove:
		w.evnets <- e
	case fsnotify.Create:
		info, err := os.Stat(e.Name)
		if err != nil {
			log.Println(err)
			return
		}

		if info.IsDir() {
			if e.Op == fsnotify.Create {
				_ = w.watcher.Add(e.Name)
			} else if e.Op == fsnotify.Remove {
				_ = w.watcher.Remove(e.Name)
			}
		}

		w.evnets <- e
	case fsnotify.Write:
		w.evnets <- e
	case fsnotify.Rename:
		w.evnets <- e
	}
}

func (w *FolderWatcher) Events() chan fsnotify.Event {
	return w.evnets
}
