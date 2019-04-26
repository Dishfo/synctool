package fswatcher

import "strconv"

type FileOp int

const (
	CREATE = 1 << iota
	MOVE
	MOVETO
	REMOVE
	WRITE
	IGNORED
)

type Event struct {
	Op   FileOp
	Name string
}

func (op FileOp) String() string {
	switch op {
	case CREATE:
		return "Create"
	case MOVE:
		return "Move"
	case MOVETO:
		return "Moveto"
	case REMOVE:
		return "Remove"
	case WRITE:
		return "Write"
	case IGNORED:
		return "Ingnore"
	default:
		return "Unknow" + strconv.Itoa(int(op))
	}
}
