package vxproto

import (
	"errors"
	"sync"
	"time"
)

const (
	defQueueSize int = 100
)

type recvBlocker struct {
	retChan chan *Packet
	pType   PacketType
	src     string
}

type recvRouter struct {
	apiMutex *sync.Mutex
	receiver chan *Packet
	queue    []*recvBlocker
}

func newRouter() *recvRouter {
	return &recvRouter{
		receiver: make(chan *Packet, defQueueSize),
		apiMutex: &sync.Mutex{},
	}
}

func (r *recvRouter) routePacket(packet *Packet) {
	r.apiMutex.Lock()
	defer r.apiMutex.Unlock()
	for idx, b := range r.queue {
		if (b.src == packet.Src || b.src == "") && b.pType == packet.PType {
			b.retChan <- packet
			// It's possible because there used return in the bottom
			r.queue = append(r.queue[:idx], r.queue[idx+1:]...)
			return
		}
	}
	r.receiver <- packet
}

func (r *recvRouter) unlock(src string) {
	r.apiMutex.Lock()
	defer r.apiMutex.Unlock()
	for _, b := range r.queue {
		if b.src == src {
			select {
			case b.retChan <- nil:
			default:
			}
		}
	}
}

func (r *recvRouter) unlockAll() {
	r.apiMutex.Lock()
	defer r.apiMutex.Unlock()
	for _, b := range r.queue {
		select {
		case b.retChan <- nil:
		default:
		}
	}
}

func (r *recvRouter) addBlocker(blocker *recvBlocker) {
	r.apiMutex.Lock()
	defer r.apiMutex.Unlock()
	r.queue = append(r.queue, blocker)
}

func (r *recvRouter) delBlocker(blocker *recvBlocker) {
	r.apiMutex.Lock()
	defer r.apiMutex.Unlock()
	for idx, b := range r.queue {
		if b == blocker {
			// It's possible because there used return in the bottom
			r.queue = append(r.queue[:idx], r.queue[idx+1:]...)
			return
		}
	}
}

func (r *recvRouter) recvPacket(src string, pType PacketType, timeout int64) (*Packet, error) {
	var packet *Packet
	blocker := &recvBlocker{
		retChan: make(chan *Packet),
		src:     src,
		pType:   pType,
	}
	r.addBlocker(blocker)
	defer r.delBlocker(blocker)
	if timeout > 0 {
		select {
		case <-time.NewTimer(time.Millisecond * time.Duration(timeout)).C:
			return nil, errors.New("timeout exceeded")
		case packet = <-blocker.retChan:
		}
	} else if timeout == 0 {
		select {
		case packet = <-blocker.retChan:
		default:
			return nil, errors.New("nothing in queue")
		}
	} else {
		packet = <-blocker.retChan
	}
	if packet == nil {
		return nil, errors.New("connection reset")
	}

	return packet, nil
}
