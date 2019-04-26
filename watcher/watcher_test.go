package watcher

import (
	"log"
	"testing"
)

const (
	testPath = "/home/dishfo/test2"
)

func TestFileWatchr_SetFolder(t *testing.T) {
	w, err := NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	err = w.SetFolder(testPath)
	log.Println(err)
	for {
		select {
		case e := <-w.evnets:
			log.Println(e)
		}
	}

}

func TestFileWatchr_Close(t *testing.T) {
	log.Println("\000")
}

func TestFileWatchr_GetAll(t *testing.T) {

}
