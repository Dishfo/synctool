package fs

import (
	"sync"
	"syncfolders/fswatcher"
)

/**
添加id 字段用于描述event 先后顺序
诉求:
可以读写event
可以获取没有处理的event
(
 	既可以按顺序获取所有的event
 	也可以分别获取一个文件的所有事件
)

提供标记位来表示上次处理的最后一个事件

ps:
关于事件处理的逻辑,
delete事件是及时处理
rename与create 都不会纳入集合
只会导致fileList的变化

rename需要进行解析寻找失去的文件
修正fileList内容
并且会产生一个delete fileinfo ,表示这个文件不再可用
todo 关于如何获取文件原名的方式 暂时没有稳定的方法
对于这个源文件的事件都应该考虑丢弃，事件难以通过 fileinfo
进行记录.将先前的事件移至新的文件名下,但是rename,rename
事件不进行保存

并继续往后遍历，直	到没有事件,
有两种情况.

1 delete,rename ,导致在原有文件不存在
2 没有其他的修改

如果没有对应的item 表示文件已经丢失
如果有表示文件还存在
构建对应的fileinfo

尽量考虑使用内存进行存储,
todo 为了防止使用过多的内存，一定要有移除行为
 遍历单个文件的事件,期望可以从上次处理的事件后
 全局的evnet 似乎没有意义

type EventSet struct {
	events map[string]int
}

handle Event {
swicth e.OP {
case write:
 events[e.Name] = write
case delete:
 events[e.Name] = delete
 remove(fileList,e.Name)
 info := generateInfo(e.Name,delete)
 id := sotreInfo(info)
 rememberUpadte(id)
case rename:
 f := findSourcee(e)
 events[f] = delete
 info := generateInfo(f,delete)
 id := sotreInfo(info)
 rememberUpadte(id)
 events[e.Name] = rename
case create:
 events[e.Name] = create
}
}


calculateUpdate {
infoIds := make([]int64,0)
for k,e := events {
if e == delete {
continue
}else if e == rename {
	info = generateInfo(k)
	id := sotreInfo(info)
	infoIds = appeand(infoIds,id )
}else if e== wirte {
	info = generateInfo(k)
	id := sotreInfo(info)
	infoIds = appeand(infoIds,id )
}else if e == create  {
	info = generateInfo(k)
	id := sotreInfo(info)
	infoIds = appeand(infoIds,id )
}

uid := storeUpdate(infoIdss
}

*/

type WrappedEvent struct {
	fswatcher.Event
	Mods  int64
	ModNs int64
}

//手工实现的list item
type Event struct {
	WrappedEvent
	Next *Event
	Pre  *Event
}

type EventList struct {
	Name string
	Head *Event
	Tail *Event
	lock sync.RWMutex
}

func NewList() *EventList {
	el := new(EventList)
	el.Head = new(Event)
	el.Tail = el.Head
	return el
}

func (el *EventList) NewEvent(e WrappedEvent) {
	el.lock.Lock()
	defer el.lock.Unlock()
	eptr := new(Event)
	eptr.WrappedEvent = e
	el.Tail.Next = eptr
	el.Tail.Pre = el.Tail
	el.Tail = eptr
}

func (el *EventList) Front() *Event {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.Head.Next
}

func (el *EventList) Back() *Event {
	el.lock.RLock()
	defer el.lock.RUnlock()
	if el.Head == el.Tail {
		return nil
	} else {
		return el.Tail
	}
}

//BackWard will Discard pre  node of element
func (el *EventList) BackWard(element *Event) {
	el.lock.Lock()
	defer el.lock.Unlock()
	el.Head.Next = element.Next
	element.Pre = el.Head
	if el.Head.Next == nil {
		el.Tail = el.Head
	} else {
		el.Tail = element.Back()
	}
}

func (el *EventList) Clear() {
	el.Head = new(Event)
	el.Tail = el.Head
}

//
func (e *Event) Back() *Event {
	ptr := e
	old := ptr
	for ptr != nil {
		old = ptr
		ptr = ptr.Next
	}
	return old
}

func (el *EventList) Next(e *Event) *Event {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return e.Next
}

func (el *EventList) Pre(e *Event) *Event {
	el.lock.RLock()
	defer el.lock.RUnlock()
	if e.Pre == el.Head {
		return nil
	} else {
		return e.Pre
	}
}

/**
多个list集合
*/
type EventSet struct {
	lists map[string]*EventList
	lock  sync.RWMutex
}

func NewEventSet() *EventSet {
	es := new(EventSet)
	es.lists = make(map[string]*EventList)
	return es
}

//
func (es *EventSet) NewEvent(e WrappedEvent) {
	es.lock.Lock()
	el, ok := es.lists[e.Name]
	if !ok {
		el = NewList()
		el.Name = e.Name
	}
	es.lists[e.Name] = el
	es.lock.Unlock()
	el.NewEvent(e)
}

//关键的遍历函数
func (es *EventSet) AvailableList() []*EventList {
	es.lock.Lock()
	defer es.lock.Unlock()
	lists := make([]*EventList, 0)
	for _, l := range es.lists {
		if l.Front() != nil {
			lists = append(lists, l)
		}
	}
	return lists
}
