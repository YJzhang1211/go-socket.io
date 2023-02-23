package socketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
)

func newRedisBroadcastRemote(
	nsp string, opts *RedisAdapterOptions,
	rbcLocal *redisBroadcastLocal,
) (*redisBroadcastRemote, error) {
	addr := opts.getAddr()
	var redisOpts []redis.DialOption
	if len(opts.Password) > 0 {
		redisOpts = append(redisOpts, redis.DialPassword(opts.Password))
	}

	pub, err := redis.Dial(opts.Network, addr, redisOpts...)
	if err != nil {
		return nil, err
	}

	sub, err := redis.Dial(opts.Network, addr, redisOpts...)
	if err != nil {
		return nil, err
	}

	subConn := &redis.PubSubConn{Conn: sub}
	pubConn := &redis.PubSubConn{Conn: pub}

	if err = subConn.PSubscribe(fmt.Sprintf("%s#%s#*", opts.Prefix, nsp)); err != nil {
		return nil, err
	}

	rbc := &redisBroadcastRemote{
		sub:        subConn,
		pub:        pubConn,
		reqChannel: fmt.Sprintf("%s-request#%s", opts.Prefix, nsp),
		resChannel: fmt.Sprintf("%s-response#%s", opts.Prefix, nsp),
		key:        fmt.Sprintf("%s#%s#%s", opts.Prefix, nsp, rbcLocal.uid),
		local:      rbcLocal,
		requests:   make(map[string]interface{}),
	}

	if err = subConn.Subscribe(rbc.reqChannel, rbc.resChannel); err != nil {
		return nil, err
	}

	// FIXME: review this concurrent
	go rbc.dispatch()

	return rbc, nil
}

type redisBroadcastRemote struct {
	pub        *redis.PubSubConn
	sub        *redis.PubSubConn
	key        string
	reqChannel string
	resChannel string
	requests   map[string]interface{}
	local      *redisBroadcastLocal
}

func (bc *redisBroadcastRemote) lenRoom(room string) int {
	req := roomLenRequest{
		RequestType: roomLenReqType,
		RequestID:   newV4UUID(),
		Room:        room,
	}

	reqJSON, err := json.Marshal(&req)
	if err != nil {
		return -1
	}

	numSub, err := bc.getNumSub(bc.reqChannel)
	if err != nil {
		return -1
	}

	req.numSub = numSub

	req.done = make(chan bool, 1)

	bc.requests[req.RequestID] = &req
	_, err = bc.pub.Conn.Do("PUBLISH", bc.reqChannel, reqJSON)
	if err != nil {
		return -1
	}

	<-req.done

	delete(bc.requests, req.RequestID)
	return req.connections
}

func (bc *redisBroadcastRemote) send(room string, event string, args ...interface{}) {
	// FIXME: review this concurrent
	go bc.publishMessage(room, event, args...)
}
func (bc *redisBroadcastRemote) sendAll(event string, args ...interface{}) {
	// FIXME: review this concurrent
	go bc.publishMessage("", event, args...)
}
func (bc *redisBroadcastRemote) clear(room string) {
	// FIXME: review this concurrent
	go bc.publishClear(room)
}
func (bc *redisBroadcastRemote) allRooms() []string {
	req := allRoomRequest{
		RequestType: allRoomReqType,
		RequestID:   newV4UUID(),
	}
	reqJSON, _ := json.Marshal(&req)

	req.rooms = make(map[string]bool)
	numSub, _ := bc.getNumSub(bc.reqChannel)
	req.numSub = numSub
	req.done = make(chan bool, 1)

	bc.requests[req.RequestID] = &req
	_, err := bc.pub.Conn.Do("PUBLISH", bc.reqChannel, reqJSON)
	if err != nil {
		return []string{} // if error occurred,return empty
	}

	<-req.done

	rooms := make([]string, 0, len(req.rooms))
	for room := range req.rooms {
		rooms = append(rooms, room)
	}

	delete(bc.requests, req.RequestID)
	return rooms
}

func (bc *redisBroadcastRemote) onMessage(channel string, msg []byte) error {
	channelParts := strings.Split(channel, "#")
	nsp := channelParts[len(channelParts)-2]
	if bc.local.nsp != nsp {
		return nil
	}

	uid := channelParts[len(channelParts)-1]
	if bc.local.uid == uid {
		return nil
	}

	var bcMessage map[string][]interface{}
	err := json.Unmarshal(msg, &bcMessage)
	if err != nil {
		return errors.New("invalid broadcast message")
	}

	args := bcMessage["args"]
	opts := bcMessage["opts"]

	room, ok := opts[0].(string)
	if !ok {
		return errors.New("invalid room")
	}

	event, ok := opts[1].(string)
	if !ok {
		return errors.New("invalid event")
	}

	if room != "" {
		bc.local.send(room, event, args...)
	} else {
		bc.local.sendAll(event, args...)
	}

	return nil
}

// Get the number of subscribers of a channel.
func (bc *redisBroadcastRemote) getNumSub(channel string) (int, error) {
	rs, err := bc.pub.Conn.Do("PUBSUB", "NUMSUB", channel)
	if err != nil {
		return 0, err
	}

	numSub64, ok := rs.([]interface{})[1].(int64)
	if !ok {
		return 0, errors.New("redis reply cast to int error")
	}
	return int(numSub64), nil
}

// Handle request from redis channel.
func (bc *redisBroadcastRemote) onRequest(msg []byte) {
	var req map[string]string

	if err := json.Unmarshal(msg, &req); err != nil {
		return
	}

	var res interface{}
	switch req["RequestType"] {
	case roomLenReqType:
		res = roomLenResponse{
			RequestType: req["RequestType"],
			RequestID:   req["RequestID"],
			Connections: bc.local.lenRoom(req["Room"]),
		}
		bc.publish(bc.resChannel, &res)

	case allRoomReqType:
		res := allRoomResponse{
			RequestType: req["RequestType"],
			RequestID:   req["RequestID"],
			Rooms:       bc.local.allRooms(),
		}
		bc.publish(bc.resChannel, &res)

	case clearRoomReqType:
		if bc.local.uid == req["UUID"] {
			return
		}
		bc.local.clear(req["Room"])

	default:
	}
}

func (bc *redisBroadcastRemote) publish(channel string, msg interface{}) {
	resJSON, err := json.Marshal(msg)
	if err != nil {
		return
	}

	_, err = bc.pub.Conn.Do("PUBLISH", channel, resJSON)
	if err != nil {
		return
	}
}

// Handle response from redis channel.
func (bc *redisBroadcastRemote) onResponse(msg []byte) {
	var res map[string]interface{}

	err := json.Unmarshal(msg, &res)
	if err != nil {
		return
	}

	req, ok := bc.requests[res["RequestID"].(string)]
	if !ok {
		return
	}

	switch res["RequestType"] {
	case roomLenReqType:
		roomLenReq := req.(*roomLenRequest)

		roomLenReq.mutex.Lock()
		roomLenReq.msgCount++
		roomLenReq.connections += int(res["Connections"].(float64))
		roomLenReq.mutex.Unlock()

		if roomLenReq.numSub == roomLenReq.msgCount {
			roomLenReq.done <- true
		}

	case allRoomReqType:
		allRoomReq := req.(*allRoomRequest)
		rooms, ok := res["Rooms"].([]interface{})
		if !ok {
			allRoomReq.done <- true
			return
		}

		allRoomReq.mutex.Lock()
		allRoomReq.msgCount++
		for _, room := range rooms {
			allRoomReq.rooms[room.(string)] = true
		}
		allRoomReq.mutex.Unlock()

		if allRoomReq.numSub == allRoomReq.msgCount {
			allRoomReq.done <- true
		}

	default:
	}
}

func (bc *redisBroadcastRemote) publishClear(room string) {
	req := clearRoomRequest{
		RequestType: clearRoomReqType,
		RequestID:   newV4UUID(),
		Room:        room,
		UUID:        bc.local.uid,
	}

	bc.publish(bc.reqChannel, &req)
}

func (bc *redisBroadcastRemote) publishMessage(room string, event string, args ...interface{}) {
	opts := make([]interface{}, 2)
	opts[0] = room
	opts[1] = event

	bcMessage := map[string][]interface{}{
		"opts": opts,
		"args": args,
	}
	bcMessageJSON, err := json.Marshal(bcMessage)
	if err != nil {
		return
	}

	_, err = bc.pub.Conn.Do("PUBLISH", bc.key, bcMessageJSON)
	if err != nil {
		return
	}
}

func (bc *redisBroadcastRemote) dispatch() {
	for {
		switch m := bc.sub.Receive().(type) {
		case redis.Message:
			switch m.Channel {
			case bc.reqChannel:
				bc.onRequest(m.Data)
				continue
			case bc.resChannel:
				bc.onResponse(m.Data)
				continue
			default:
				err := bc.onMessage(m.Channel, m.Data)
				if err != nil {
					return
				}
			}
		case redis.Subscription:
			if m.Count == 0 {
				return
			}
		case error:
			return
		}
	}
}

// request types
const (
	roomLenReqType   = "0"
	clearRoomReqType = "1"
	allRoomReqType   = "2"
)

// request structs
type roomLenRequest struct {
	RequestType string
	RequestID   string
	Room        string
	numSub      int        `json:"-"`
	msgCount    int        `json:"-"`
	connections int        `json:"-"`
	mutex       sync.Mutex `json:"-"`
	done        chan bool  `json:"-"`
}

type clearRoomRequest struct {
	RequestType string
	RequestID   string
	Room        string
	UUID        string
}

type allRoomRequest struct {
	RequestType string
	RequestID   string
	rooms       map[string]bool `json:"-"`
	numSub      int             `json:"-"`
	msgCount    int             `json:"-"`
	mutex       sync.Mutex      `json:"-"`
	done        chan bool       `json:"-"`
}

// response struct
type roomLenResponse struct {
	RequestType string
	RequestID   string
	Connections int
}

type allRoomResponse struct {
	RequestType string
	RequestID   string
	Rooms       []string
}
