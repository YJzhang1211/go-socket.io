package socketio

import (
	"sync"
)

// roomMap as sync.Map

func newRoomMap() *roomMap {
	return &roomMap{data: make(map[string]*connMap)}
}

type roomMap struct {
	data  map[string]*connMap
	mutex sync.RWMutex
}

// join register the connection to room
func (rm *roomMap) join(room string, conn Conn) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	cm, ok := rm.data[room]
	if !ok {
		rm.data[room] = newConnMap()
		cm = rm.data[room]
	}

	cm.join(conn)
}

// leaveAll remove the connection from all rooms
func (rm *roomMap) leaveAll(conn Conn) {
	iterating := rm.iterableData()

	for room := range iterating {
		iterating[room].leave(conn)
	}
}

// leave remove the connection from the specific room
func (rm *roomMap) leave(room string, conn Conn) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	// find conn map
	cm, ok := rm.data[room]
	if !ok {
		return
	}

	cm.leave(conn)
	if cm.len() == 0 {
		delete(rm.data, room)
	}
}

// delete remove the specific room
func (rm *roomMap) delete(room string) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	delete(rm.data, room)
}

// getConnections return connMap for specific room
func (rm *roomMap) getConnections(room string) (*connMap, bool) {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	cm, ok := rm.data[room]
	return cm, ok
}

// forEach is use for iterate for read purpose only
func (rm *roomMap) forEach(h func(room string, conn *connMap) bool) {
	iterating := rm.iterableData()

	for room := range iterating {
		if !h(room, iterating[room]) {
			break
		}
	}
}

// iterableData return a version of data that is suitable for handle concurrent iteration
func (rm *roomMap) iterableData() map[string]*connMap {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	return copyMap(rm.data)
}

// ====================================
// ===========CONN MAP=================
// ====================================

func newConnMap() *connMap {
	return &connMap{data: make(map[string]Conn)}
}

type connMap struct {
	mutex sync.RWMutex
	data  map[string]Conn
}

func (cm *connMap) join(conn Conn) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.data[conn.ID()] = conn
}

func (cm *connMap) leave(conn Conn) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	delete(cm.data, conn.ID())
}

// forEach is use for iterate for read purpose only
func (cm *connMap) forEach(h func(connID string, conn Conn) bool) {
	iterating := cm.iterableData()

	for connID := range iterating {
		if !h(connID, iterating[connID]) {
			break
		}
	}
}

// iterableData return a version of data that is suitable for handle concurrent iteration
func (cm *connMap) iterableData() map[string]Conn {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return copyMap(cm.data)
}

// len return the current size of the map
func (cm *connMap) len() int {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return len(cm.data)
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	res := make(map[K]V, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}
