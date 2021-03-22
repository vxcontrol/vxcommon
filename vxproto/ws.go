package vxproto

import (
	"errors"

	"github.com/gorilla/websocket"
)

// wsConnection is implementatation of websocket connection for IConnection
type wsConnection struct {
	*websocket.Conn
}

// Read is synchronous function that provides interaction with the websocket API
func (ws *wsConnection) Read() (data []byte, err error) {
	msgType, msgData, err := ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	if msgType != websocket.BinaryMessage {
		return nil, errors.New("unexpected message type")
	}

	return msgData, nil
}

// Write is synchronous function that provides interaction with the websocket API
func (ws *wsConnection) Write(data []byte) error {
	return ws.WriteMessage(websocket.BinaryMessage, data)
}
