package socketio

// redisBroadcast gives Join, Leave & BroadcastTO server API support to socket.io along with room management
// map of rooms where each room contains a map of connection id to connections in that room
type redisBroadcast struct {
	remote *redisBroadcastRemote
	local  *redisBroadcastLocal
}

func newRedisBroadcast(nsp string, opts *RedisAdapterOptions) (*redisBroadcast, error) {
	rbcLocal := newRedisBroadcastLocal(nsp)
	rbcRemote, err := newRedisBroadcastRemote(nsp, opts, rbcLocal)
	if err != nil {
		return nil, err
	}

	return &redisBroadcast{
		remote: rbcRemote,
		local:  rbcLocal,
	}, nil
}

// AllRooms gives list of all rooms available for redisBroadcast.
func (bc *redisBroadcast) AllRooms() []string {
	return bc.remote.allRooms()
}

// Join joins the given connection to the redisBroadcast room.
func (bc *redisBroadcast) Join(room string, conn Conn) {
	bc.local.join(room, conn)
}

// Leave leaves the given connection from given room (if exist)
func (bc *redisBroadcast) Leave(room string, conn Conn) {
	bc.local.leave(room, conn)
}

// LeaveAll leaves the given connection from all rooms.
func (bc *redisBroadcast) LeaveAll(conn Conn) {
	bc.local.leaveAll(conn)
}

// Clear clears the room.
func (bc *redisBroadcast) Clear(room string) {
	bc.local.clear(room)
	bc.remote.clear(room)
}

// Send sends given event & args to all the connections in the specified room.
func (bc *redisBroadcast) Send(room, event string, args ...interface{}) {
	bc.local.send(room, event, args...)
	bc.remote.send(room, event, args...)
}

// SendAll sends given event & args to all the connections to all the rooms.
func (bc *redisBroadcast) SendAll(event string, args ...interface{}) {
	bc.local.sendAll(event, args...)
	bc.remote.sendAll(event, args...)
}

// ForEach sends data returned by DataFunc, if room does not exit sends anything.
func (bc *redisBroadcast) ForEach(room string, f EachFunc) {
	bc.local.forEach(room, f)
}

// Len gives number of connections in the room.
func (bc *redisBroadcast) Len(room string) int {
	return bc.remote.lenRoom(room)
}

// Rooms gives the list of all the rooms available for redisBroadcast in case of
// no connection is given, in case of a connection is given, it gives
// list of all the rooms the connection is joined to.
func (bc *redisBroadcast) Rooms(connection Conn) []string {
	if connection == nil {
		return bc.AllRooms()
	}

	return bc.local.getRoomsByConn(connection)
}
