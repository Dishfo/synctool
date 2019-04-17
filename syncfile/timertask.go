package syncfile

import "time"

/**
提供定时任务支持
*/
type Action func()
type TaskType int16

const (
	TASK_ONCE = 0
	TASK_DUR  = 1
)

//用于设置定时任务
type Task struct {
	Act  Action
	Type TaskType
	Dur  int64
}

type TaskManager struct {
	close chan int
}

func (tm *TaskManager) AddTask(task Task) {
	if task.Act == nil {
		return
	}

	if task.Dur < 0 {
		return
	}

	timer := time.NewTimer(time.Duration(task.Dur))
	go func() {
		for {
			select {
			case <-tm.close:
				return
			default:
			}

			select {
			case <-timer.C:
				task.Act()
				if task.Type == TASK_DUR {
					timer.Reset(time.Duration(task.Dur))
				} else {
					return
				}
			}
		}
	}()
}

func (tm *TaskManager) Close() {
	close(tm.close)
}

func NewTaskManager() *TaskManager{
	tm := new(TaskManager)
	tm.close = make(chan int)
	return tm
}




