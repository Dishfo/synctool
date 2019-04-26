package fswatcher

import (
	"errors"
	"golang.org/x/sys/unix"
)

/**
使用unix 提供的epoll 接口来完后对于fd的读写逻辑
*/

type fdPoller struct {
	fd    int
	eFd   int
	pipes [2]int
}

func newPoller(fd int) (*fdPoller, error) {
	var err error
	poller := &fdPoller{
		fd:    -1,
		eFd:   -1,
		pipes: [2]int{-1, -1},
	}

	poller.fd = fd

	poller.eFd, err = unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	err = unix.Pipe2(poller.pipes[:], unix.O_NONBLOCK)
	if err != nil {
		return nil, err
	}

	event := &unix.EpollEvent{
		Fd:     int32(fd),
		Events: unix.EPOLLIN,
	}

	err = unix.EpollCtl(poller.eFd,
		unix.EPOLL_CTL_ADD, fd, event)
	if err != nil {
		return nil, err
	}

	event = &unix.EpollEvent{
		Fd:     int32(poller.pipes[0]),
		Events: unix.EPOLLIN,
	}
	err = unix.EpollCtl(poller.eFd,
		unix.EPOLL_CTL_ADD, poller.pipes[0], event)
	if err != nil {
		return nil, err
	}

	//EPOLLHUP EPOLLERR always set ,不需要手动设置
	return poller, err
}

func (ep *fdPoller) wait() (bool, error) {
	events := make([]unix.EpollEvent, 10)

	for {
		n, err := unix.EpollWait(ep.eFd, events, -1)
		if n == -1 {
			if err == unix.EINTR {
				continue
			}
			return false, err
		}

		ready := events[:n]
		pa := false
		for _, event := range ready {
			if event.Fd == int32(ep.fd) {
				pa = true
			}

			if event.Fd == int32(ep.pipes[0]) {
				if event.Events&unix.EPOLLHUP != 0 {

				}
				if event.Events&unix.EPOLLERR != 0 {
					return false, errors.New("Error on the pipe descriptor.")
				}
				if event.Events&unix.EPOLLIN != 0 {
					err := ep.clearWake()
					if err != nil {
						return false, err
					}
				}
			}
		}

		if pa {
			return true, nil
		}

		return false, nil
	}
}

func (ep *fdPoller) wake() error {
	buf := make([]byte, 1)
	n, err := unix.Write(ep.pipes[1], buf)
	if n == -1 {
		if err == unix.EAGAIN {
			return nil
		}
		return err
	}
	return nil
}

func (ep *fdPoller) clearWake() error {
	buf := make([]byte, 2)
	n, err := unix.Read(ep.pipes[0], buf)
	if n == -1 {
		if err == unix.EAGAIN {
			return nil
		}
		return err
	}
	return nil
}

func (ep *fdPoller) close() {
	if ep.pipes[1] != -1 {
		_ = unix.Close(ep.fd)
		ep.pipes[1] = 1
	}

	if ep.pipes[0] != -1 {
		_ = unix.Close(ep.fd)
		ep.pipes[0] = 1
	}

	if ep.eFd != -1 {
		_ = unix.Close(ep.eFd)
		ep.eFd = -1
	}
}
