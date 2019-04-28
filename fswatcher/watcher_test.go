package fswatcher

import (
	"golang.org/x/sys/unix"
	"log"
	"testing"
)

const (
	filePath = "/home/dishfo/test2"
)

//inotify 返回的 wd 是非阻塞的描述符
func TestUnixWatcher(t *testing.T) {
	fd, err := unix.InotifyInit()
	if err != nil {
		log.Fatal(err)
	}

	flag, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(flag | unix.O_NONBLOCK)
}

func TestWatcher(t *testing.T) {
	w, err := NewWatcher(filePath)
	if err != nil {
		log.Fatal(err)
	}
	//go func() {
	//	timer := time.NewTimer(time.Second * 5)
	//	select {
	//	case <-timer.C:
	//		w.Close()
	//	}
	//}()
	for {
		select {
		case err, ok := <-w.errors:
			if !ok {
				return
			}
			log.Println(err)
		case ev, ok := <-w.events:
			if !ok {
				return
			}
			log.Println(ev)
		}
	}
}

func TestPoller(t *testing.T) {
	fd, err := unix.Open(filePath+"/"+"fileA", unix.O_RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}
	flag, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		log.Fatal(err)
	}

	_, err = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flag|unix.O_NONBLOCK)

	if err != nil {
		log.Fatal(err)
	}

	ep, err := newPoller(fd)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(ep.wait())

}
