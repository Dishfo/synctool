package fs

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"syncfolders/bep"
	"syncfolders/node"
	"testing"
	"time"
)

func TestNewSet(t *testing.T) {
}

func TestMarshalBlocks(t *testing.T) {
	blocks := []*bep.BlockInfo{
		{
			Size: 15,
		},
		{
			Size: 177,
		},
	}

	p, err := marshalBlcoks(blocks)
	if err != nil {
		log.Fatal(err)
	}

	blocks = unmarshalBlcoks(p)
	for _, b := range blocks {
		log.Println(*b)
	}
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
	fs := new(FileSystem)
	fs.folders = make(map[string]*FolderNode)
	err := fs.AddFolder("default", "/home/dishfo/test2")
	if err != nil {
		log.Fatal(err)
	}
	index := fs.GetIndex("default")
	data, _ := json.MarshalIndent(index, "", " ")
	log.Println(string(data))
	select {}
}

func TestFileInfo(t *testing.T) {
	info, err := GenerateFileInfo("/home/dishfo/test2/hello")
	if err != nil {
		log.Fatal(err)
	} else {
		info.Version = &bep.Vector{
			Counters: []*bep.Counter{
				{Id: 12345,
					Value: 13},
			},
		}
		info.Name = "testaa"
		log.Println(info)
	}

	id, _ := StoreFileinfo(nil, "default", info)
	info, _ = GetInfoById(nil, id)
	log.Println(info)
}

func TestFileSystem_GetFileList(t *testing.T) {
	log.Println(getRealFileList("/home/dishfo/test2"))
}

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
}
