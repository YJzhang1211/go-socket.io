package socketio

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/vchitai/go-socket.io/v4/parser"
)

type namespaceHandler struct {
	broadcast Broadcast

	events     map[string]*funcHandler
	eventsLock sync.RWMutex

	onConnect    func(conn Conn, req map[string]interface{}) error
	onDisconnect func(conn Conn, msg string, details map[string]interface{})
	onError      func(conn Conn, err error)
}

func newNamespaceHandler(nsp string, adapterOpts *RedisAdapterOptions) *namespaceHandler {
	var broadcast Broadcast
	if adapterOpts == nil {
		broadcast = newBroadcast()
	} else {
		ctx := context.TODO()
		broadcast, _ = newRedisV8Broadcast(ctx, nsp, adapterOpts)
	}

	return &namespaceHandler{
		broadcast: broadcast,
		events:    make(map[string]*funcHandler),
	}
}

func (nh *namespaceHandler) OnConnect(f func(Conn, map[string]interface{}) error) {
	nh.onConnect = f
}

func (nh *namespaceHandler) OnDisconnect(f func(Conn, string, map[string]interface{})) {
	nh.onDisconnect = f
}

func (nh *namespaceHandler) OnError(f func(Conn, error)) {
	nh.onError = f
}

func (nh *namespaceHandler) OnEvent(event string, f interface{}) {
	nh.eventsLock.Lock()
	defer nh.eventsLock.Unlock()

	nh.events[event] = newEventFunc(f)
}

func (nh *namespaceHandler) getEventTypes(event string) []reflect.Type {
	nh.eventsLock.RLock()
	namespaceHandler := nh.events[event]
	nh.eventsLock.RUnlock()

	if namespaceHandler != nil {
		return namespaceHandler.argTypes
	}

	return nil
}

func (nh *namespaceHandler) dispatch(conn Conn, header parser.Header, args ...reflect.Value) ([]reflect.Value, error) {
	switch header.Type {
	case parser.Connect:
		if nh.onConnect != nil {
			return nil, nh.onConnect(conn, getDispatchData(args...))
		}
		return nil, nil

	case parser.Disconnect:
		if nh.onDisconnect != nil {
			reason, details := getDispatchDisconnectData(args...)
			nh.onDisconnect(conn, reason, details)
		}
		return nil, nil

	case parser.Error:
		if nh.onError != nil {
			msg := getDispatchMessage(args...)
			if msg == "" {
				msg = "parser error dispatch"
			}
			nh.onError(conn, errors.New(msg))
		}
	}

	return nil, parser.ErrInvalidPacketType
}

func (nh *namespaceHandler) dispatchEvent(conn Conn, event string, args ...reflect.Value) ([]reflect.Value, error) {
	nh.eventsLock.RLock()
	namespaceHandler := nh.events[event]
	nh.eventsLock.RUnlock()

	if namespaceHandler == nil {
		return nil, nil
	}

	return namespaceHandler.Call(append([]reflect.Value{reflect.ValueOf(conn)}, args...))
}

func getDispatchDisconnectData(args ...reflect.Value) (reason string, details map[string]interface{}) {
	if len(args) > 0 {
		reason = args[0].Interface().(string)
	}
	if len(args) > 1 {
		details = args[0].Interface().(map[string]interface{})
	}

	return
}

func getDispatchMessage(args ...reflect.Value) string {
	var msg string
	if len(args) > 0 {
		msg = args[0].Interface().(string)
	}

	return msg
}

func getDispatchData(args ...reflect.Value) map[string]interface{} {
	var val map[string]interface{}
	if len(args) > 0 {
		val = args[0].Interface().(map[string]interface{})
	}

	return val
}
