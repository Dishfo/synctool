package fs

import (
	"encoding/json"
	"path/filepath"
)

type fileType int

const (
	typeFolder = iota
	typeNormalFile
)

//输入的可靠性依赖外部
type fileList struct {
	folder   string
	real     string
	indexs   map[string]int
	items    map[int]Elem
	indexGen int
	folders  map[int]Folder
}

type Folder struct {
	name  string
	items []int
}

type Elem struct {
	name     string
	fileType fileType
}

func newFileList(folder string) *fileList {
	folder = filepath.Clean(folder)
	fl := new(fileList)
	fl.folder = folder

	fl.indexs = make(map[string]int)
	fl.items = make(map[int]Elem)

	fl.folders = make(map[int]Folder)
	fl.indexGen = 0

	fl.items[fl.indexGen] = Elem{
		fileType: typeFolder,
		name:     folder,
	}

	fl.folders[0] = newFolder(folder)
	fl.indexs[folder] = 0

	fl.indexGen += 1
	return fl
}

func newFolder(folder string) Folder {
	return Folder{
		name:  folder,
		items: []int{},
	}
}

func (fl *fileList) getItems() []string {
	items := make([]string, 0)
	for k := range fl.indexs {
		items = append(items, k)
	}
	return items
}

func (fl *fileList) newFile(name string) {
	var index int
	parent := filepath.Dir(name)
	name = filepath.Clean(name)
	if i, ok := fl.indexs[parent]; ok {
		index, fl.indexGen = fl.indexGen, fl.indexGen+1
		fl.indexs[name] = index
		fl.items[index] = Elem{
			fileType: typeNormalFile,
			name:     name,
		}
		elem := fl.items[i]
		if elem.fileType == typeFolder {
			folder := fl.folders[i]
			folder.items = append(folder.items, index)
			fl.folders[i] = folder
		}
	} else {
		return
	}
}

func (fl *fileList) newFolder(name string) {
	var index int
	parent := filepath.Dir(name)
	name = filepath.Clean(name)
	if i, ok := fl.indexs[parent]; ok {
		index, fl.indexGen = fl.indexGen, fl.indexGen+1
		fl.indexs[name] = index
		fl.items[index] = Elem{
			fileType: typeFolder,
			name:     name,
		}

		elem := fl.items[i]
		if elem.fileType == typeFolder {
			folder := fl.folders[i]
			folder.items = append(folder.items, index)
			fl.folders[i] = folder
		}

		fl.folders[index] = newFolder(name)
	} else {
		return
	}
}

func (fl *fileList) removeItem(name string) {
	if i, ok := fl.indexs[name]; ok {
		ele := fl.items[i]
		if ele.fileType == typeNormalFile {
			delete(fl.indexs, name)
			delete(fl.items, i)
		} else {
			delete(fl.indexs, name)
			delete(fl.items, i)
			delete(fl.folders, i)
		}
	} else {
		return
	}
}

//用于输出filelist 时使用
type item struct {
	Name  string
	Items []item
}

func dirs(fl *fileList, folder string) item {
	res := item{
		Items: []item{},
	}
	res.Name = folder
	index := fl.indexs[folder]
	f := fl.folders[index]
	for _, i := range f.items {
		ele, ok := fl.items[i]
		if !ok {
			continue
		}
		if ele.fileType == typeNormalFile {
			res.Items = append(res.Items,
				item{
					Name: ele.name,
				})
		} else {
			res.Items = append(res.Items, dirs(fl, ele.name))
		}
	}
	return res
}

func (fl *fileList) String() string {
	files := make([]interface{}, 0)

	for _, ele := range fl.items {
		if ele.name == fl.folder {
			continue
		}
		parent := filepath.Dir(ele.name)
		if parent != fl.folder {
			continue
		}
		if ele.fileType == typeNormalFile {

			files = append(files, ele.name)
		} else {
			files = append(files, dirs(fl, ele.name))
		}
	}
	data, _ := json.MarshalIndent(&files,
		"", "   ")
	return string(data)
}
