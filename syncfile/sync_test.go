package syncfile

import (
	"log"
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	log.Println("ok")

outter:
	for {
		log.Println(1)
		select {
		default:
			log.Println(1)
			continue outter
			log.Println(2)
		}
	}
}

func TestTaskManger(t *testing.T) {
	tm := NewTaskManager()

	tm.AddTask(Task{
		Type: TASK_DUR,
		Dur:  int64(time.Second * 5),
		Act: func() {
			log.Println("123456")
		},
	})
	timer := time.NewTimer(time.Second * 20)
	select {
	case <-timer.C:
		tm.Close()
	}

	select {}
}

func TestFileOp(t *testing.T) {
	bak, err := deleteFile("/home/dishfo/test2/fileA", true)
	if err != nil {
		log.Fatal(err)
	}
	restoreBak(bak)

	bak, err = deleteFolder("/home/dishfo/test2/dir1", true)
	if err != nil {
		log.Fatal(err)
	}
	restoreBak(bak)

}
