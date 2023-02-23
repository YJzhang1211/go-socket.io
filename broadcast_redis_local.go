package socketio

import (
	"sync"
)

func newRedisBroadcastLocal(nsp string) *redisBroadcastLocal {
	uid := newV4UUID()
	return &redisBroadcastLocal{
		nsp:       nsp,
		uid:       uid,
		roomsSync: newRoomMap(),
	}
}

type redisBroadcastLocal struct {
	nsp string
	uid string

	roomsSync *roomMap
}

func (bc *redisBroadcastLocal) forEach(room string, f EachFunc) {
	occupants, ok := bc.getOccupants(room)
	if !ok {
		return
	}

	occupants.forEach(func(_ string, conn Conn) bool {
		f(conn)
		return true
	})
}

// getOccupants return all occupants of a room
func (bc *redisBroadcastLocal) getOccupants(room string) (*connMap, bool) {
	return bc.roomsSync.getConnections(room)
}

func (bc *redisBroadcastLocal) clear(room string) {
	bc.roomsSync.delete(room)
}

func (bc *redisBroadcastLocal) join(room string, conn Conn) {
	bc.roomsSync.join(room, conn)
}

func (bc *redisBroadcastLocal) leaveAll(conn Conn) {
	bc.roomsSync.leaveAll(conn)
}

func (bc *redisBroadcastLocal) leave(room string, conn Conn) {
	bc.roomsSync.leave(room, conn)
}

func (bc *redisBroadcastLocal) send(room string, event string, args ...interface{}) {
	conns, ok := bc.getOccupants(room)
	if !ok {
		return
	}
	conns.forEach(func(_ string, conn Conn) bool {
		conn.Emit(event, args...)
		return true
	})
}

func (bc *redisBroadcastLocal) sendAll(event string, args ...interface{}) {
	bc.roomsSync.forEach(func(_ string, conn *connMap) bool {
		conn.forEach(func(_ string, conn Conn) bool {
			conn.Emit(event, args...)
			return true
		})
		return true
	})
}

func (bc *redisBroadcastLocal) allRooms() []string {
	rooms := make([]string, 0)
	bc.roomsSync.forEach(func(room string, conn *connMap) bool {
		rooms = append(rooms, room)
		return true
	})
	return rooms
}

func (bc *redisBroadcastLocal) lenRoom(roomID string) int {
	var res int
	bc.roomsSync.forEach(func(room string, conn *connMap) bool {
		if room == roomID {
			res++
		}
		return true
	})
	return res
}

func (bc *redisBroadcastLocal) getRoomsByConn(connection Conn) []string {
	var rooms []string
	bc.roomsSync.forEach(func(room string, conn *connMap) bool {
		conn.forEach(func(connID string, conn Conn) bool {
			if connection.ID() == connID {
				rooms = append(rooms, room)
			}
			return true
		})
		return true
	})
	return rooms
}

func newRoomMap() *roomMap {
	return &roomMap{}
}

type roomMap struct {
	sync.Map
}

func (rm *roomMap) join(room string, conn Conn) {
	rawCm, _ := rm.LoadOrStore(room, newConnMap())
	cm, ok := rawCm.(*connMap)
	if !ok {
		return
	}
	cm.join(conn)
}

func (rm *roomMap) leave(room string, conn Conn) {
	rawCm, ok := rm.Load(room)
	if !ok {
		return
	}
	cm, ok := rawCm.(*connMap)
	if !ok {
		return
	}
	cm.leave(conn)
}

func (rm *roomMap) leaveAll(conn Conn) {
	rm.forEach(func(_ string, cm *connMap) bool {
		cm.leave(conn)
		return true
	})
}

func (rm *roomMap) delete(room string) {
	rm.Delete(room)
}

func (rm *roomMap) getConnections(room string) (*connMap, bool) {
	occupants, ok := rm.Load(room)
	if !ok {
		return nil, false
	}
	res, ok := occupants.(*connMap)
	if !ok {
		return nil, false
	}
	return res, true
}

func (rm *roomMap) forEach(h func(room string, conn *connMap) bool) {
	rm.Range(func(rawKey, rawVal any) bool {
		key, ok := rawKey.(string)
		if !ok {
			// continue on next
			return true
		}
		val, ok := rawVal.(*connMap)
		if !ok {
			// continue on next
			return true
		}
		return h(key, val)
	})
}

func newConnMap() *connMap {
	return &connMap{}
}

type connMap struct {
	sync.Map
}

func (cm *connMap) join(conn Conn) {
	cm.Store(conn.ID(), conn)
}

func (cm *connMap) leave(conn Conn) {
	cm.Delete(conn.ID())
}

func (cm *connMap) forEach(h func(connID string, conn Conn) bool) {
	cm.Range(func(rawKey, rawVal any) bool {
		key, ok := rawKey.(string)
		if !ok {
			// continue on next
			return true
		}
		val, ok := rawVal.(Conn)
		if !ok {
			// continue on next
			return true
		}
		return h(key, val)
	})
}
