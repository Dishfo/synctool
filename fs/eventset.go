package fs

import (
	"sort"
	"sync"
	"syncfolders/fswatcher"
)

/**
eventSet 用与暂时存储 文件上发生的事件
程序定期得取出这些事件,保证每次处理后的事件都会被移除
但是如果在处理中发生了错误,那么这些事件还会被保存下来.
直到下一次处理完成,
这个部分打乱了事件的先后顺序
我们先面向处理的是

上诉问题已在程序中体现出来
无法有效的顺利构建filelist
*/

/**
暂时保留此处的锁
 todo 修改内部的数据结构
  同一文件的事件放置与同一队列里
  事件的接收顺序将会标记队列的优先级

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
	lists      map[string]*EventList
	timeStamps map[string]int64
	lock       sync.RWMutex
}

func NewEventSet() *EventSet {
	es := new(EventSet)
	es.timeStamps = make(map[string]int64)
	es.lists = make(map[string]*EventList)
	return es
}

func (es *EventSet) discardEventOfFile(file string) {
	es.lock.Lock()
	defer es.lock.Unlock()
	el, ok := es.lists[file]
	if ok {
		el.Clear()
	}
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
	es.timeStamps[e.Name] = e.ModNs
	es.lock.Unlock()
	el.NewEvent(e)
}

//关键的遍历函数
//todo 对结果进行排序 将名字
func (es *EventSet) AvailableList() []*EventList {
	es.lock.Lock()
	defer es.lock.Unlock()
	lists := make([]*EventList, 0)

	for _, l := range es.lists {
		if l.Front() != nil {
			lists = append(lists, l)
		}
	}

	sort.Sort(eList{
		timeStamps: es.timeStamps,
		lists:      lists,
	})

	return lists
}

type eList struct {
	lists      []*EventList
	timeStamps map[string]int64
}

func (e eList) Less(i, j int) bool {
	nameI := e.lists[i].Name
	nameJ := e.lists[j].Name
	return e.timeStamps[nameI] < e.timeStamps[nameJ]
}

func (e eList) Len() int {
	return len(e.lists)
}

func (e eList) Swap(i, j int) {
	e.lists[i], e.lists[j] = e.lists[j], e.lists[i]
}
