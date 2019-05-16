package fs

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"syncfolders/node"
	"syncfolders/tools"
	"testing"
	"time"
)

func TestNewSet(t *testing.T) {
}

func TestMarshalBlocks(t *testing.T) {

}

//unthread-safe
func logAllList(es *EventSet) {
	log.Println("out set content")
	if es == nil {
		return
	}

	lists := es.AvailableList()
	for _, l := range lists {
		e := l.Back()
		log.Println(e)
		l.BackWard(e)
	}
}

func TestFs(t *testing.T) {
	LocalUser, _ = node.NewUnqueId()
	fs := NewFileSystem()
	err := fs.AddFolder("default", "/home/dishfo/test2/dir1")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		/*fs.StartUpdateTransaction("default")

		log.Println("open transaction 1")
		fs.StartUpdateTransaction("default")
		log.Println("open transaction 2")*/
	}()
	/*fn := fs.folders["default"]
	go func() {
		//	var lastUpdate int64 = 1
		defer tools.MethodExecTime("mmapTime")()
		tx,_ := fn.dw.GetTx()
		for i := 0; i < 50000; i++ {


			bep.GetInfoById(tx,666)

		}
		tx.Commit()
	}()*/

	select {}
}

//用于测试 生成fileinfo 的函数执行效率
func TestFileInfo(t *testing.T) {
	files := GetSubFiles("/home/dishfo/test2/dir1")
	/*	files := []string{
		"/home/dishfo/mydata/os-images/archlinux-2018.11.01-x86_64.iso",
		"/home/dishfo/mydata/os-images/Remix_OS_2_0_513.iso",
		"/home/dishfo/mydata/os-images/manjaro-xfce-18.0.4-stable-x86_64.iso",
		"/home/dishfo/mydata/os-images/elementaryos-5.0-stable.20181016.iso",
	}*/
	defer tools.MethodExecTime("all blcoks ")()
	for _, f := range files {
		finfo, _ := os.Stat(f)
		bs := selectBlockSize(finfo.Size())
		_, err := calculateBlocksBySeek(f, bs)
		if err != nil {
			log.Println(err)
		}
	}

}

func TestFileSystem_GetFileList(t *testing.T) {}

func runTime() func() {
	now := time.Now()

	return func() {
		exit := time.Now()
		log.Println(exit.Sub(now).Nanoseconds())
	}

}

func TestWordCount(t *testing.T) {
	file, err := os.Open("/home/dishfo/test2/fileA")
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	data, _ := ioutil.ReadAll(file)
	str := string(data)
	strs := strings.Split(str, " ")

	for i := 0; i < 10; i++ {
		wordCount2(strs)
	}

}

func TestMMap(t *testing.T) {
}

func wordCount1(ws []string) map[string]int {
	defer runTime()()
	words := make(map[string]int)
	for _, word := range ws {
		words[word]++
	}
	return words
}

//受数据特征的影响
func wordCount2(w []string) map[string]int {
	defer runTime()()
	words := make(map[string]int)
	workNum := runtime.NumCPU()

	var wg sync.WaitGroup
	reschl := make(chan map[string]int, workNum)

	step := len(w) / workNum
	for i := 0; i < workNum; i++ {
		index := i
		go func() {
			part := make(map[string]int)
			ws := w[index*step : (index+1)*step]
			for _, word := range ws {
				part[word]++
			}
			reschl <- part
		}()
	}

	wg.Add(1)
	go func() {
		mapnum := 0
		for mapnum < workNum {
			select {
			case m := <-reschl:
				for k, v := range m {
					words[k] = words[k] + v
				}
				mapnum++
			}
		}
		wg.Done()
	}()

	wg.Wait()

	return words
}

func TestFileList(t *testing.T) {
	//fn := newFolderNode("/home/dishfo/test2")
	//err := fn.initNode()
	//if err!=nil {
	//	log.Fatal(err)
	//}
	//
	//log.Println(fn.fl.String())

	log.Println(isHide("/sync/hode/.git"))
	log.Println(isHide(".DWADAW"))
	log.Println(isHide("/sync/false"))
	log.Println(isHide("."))
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	go func() {
		time.Sleep(time.Second * 4)
		cond.Signal()
	}()

	cond.Wait()

	log.Println("one")
	go func() {
		time.Sleep(time.Second * 4)
		cond.Signal()
	}()
	cond.Wait()

	log.Println("two")
}
