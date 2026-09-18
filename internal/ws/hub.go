// Package ws 实现WebSocket Hub和房间管理
// 负责连接管理、操作广播、在线用户光标同步
package ws

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// MessageType 消息类型
type MessageType string

const (
	MsgOp         MessageType = "op"          // 操作消息
	MsgAck        MessageType = "ack"         // 操作确认
	MsgCursors    MessageType = "cursors"     // 光标位置广播
	MsgCursorMove MessageType = "cursor_move" // 光标移动
	MsgUserJoin   MessageType = "user_join"   // 用户加入
	MsgUserLeave  MessageType = "user_leave"  // 用户离开
	MsgInit       MessageType = "init"        // 初始化消息
	MsgError      MessageType = "error"       // 错误消息
	MsgSnapshot   MessageType = "snapshot"    // 快照请求
	MsgDocDeleted MessageType = "doc_deleted" // 文档被删除（移入回收站）
)

// Message WebSocket消息
type Message struct {
	Type      MessageType `json:"type"`
	DocID     string      `json:"doc_id,omitempty"`
	ClientID  string      `json:"client_id,omitempty"`
	Username  string      `json:"username,omitempty"`
	Version   int64       `json:"version,omitempty"`
	BaseVer   int64       `json:"base_version,omitempty"`
	Op        interface{} `json:"op,omitempty"`
	Content   string      `json:"content,omitempty"`
	Users     []UserInfo  `json:"users,omitempty"`
	Cursors   []Cursor    `json:"cursors,omitempty"`
	Position  int         `json:"position,omitempty"`
	Color     string      `json:"color,omitempty"`
	Error     string      `json:"error,omitempty"`
	Title     string      `json:"title,omitempty"`
	Timestamp time.Time   `json:"timestamp,omitempty"`
}

// UserInfo 用户信息
type UserInfo struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Color    string `json:"color"`
}

// Cursor 光标位置
type Cursor struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Position int    `json:"position"`
	Color    string `json:"color"`
}

// Client 表示一个WebSocket连接
type Client struct {
	Hub       *Hub
	RoomID    string
	ClientID  string
	Username  string
	Color     string
	Position  int
	Conn      *websocket.Conn
	Send      chan []byte
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// Close 幂等地关闭发送通道。通道关闭后 WritePump 会主动断开WebSocket连接。
// 多个调用方（正常离开、房间被强制关闭）并发调用也是安全的。
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.Send)
	})
}

// trySend 非阻塞地向发送通道投递数据；通道已关闭时返回false而不会panic。
func (c *Client) trySend(data []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	select {
	case c.Send <- data:
		return true
	default:
		return false
	}
}

// Room 表示一个文档房间
type Room struct {
	ID      string
	Clients map[string]*Client
	mu      sync.RWMutex
}

// Hub 管理所有房间
type Hub struct {
	Rooms      map[string]*Room
	mu         sync.RWMutex
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan *BroadcastMessage
}

// BroadcastMessage 广播消息
type BroadcastMessage struct {
	RoomID  string
	Message []byte
	Except  string // 排除的client_id（发送者）
}

// 预定义颜色列表
var colors = []string{
	"#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
	"#1abc9c", "#e67e22", "#2980b9", "#27ae60", "#c0392b",
	"#8e44ad", "#d35400", "#16a085", "#2c3e50", "#e84393",
}

// randomColor 随机选颜色
func randomColor() string {
	return colors[int(uuid.New().ID())%len(colors)]
}

// NewHub 创建Hub
func NewHub() *Hub {
	return &Hub{
		Rooms:      make(map[string]*Room),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *BroadcastMessage, 256),
	}
}

// Run 启动Hub主循环
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.addClient(client)
		case client := <-h.Unregister:
			h.removeClient(client)
		case msg := <-h.Broadcast:
			h.broadcast(msg)
		}
	}
}

// addClient 添加客户端到房间
func (h *Hub) addClient(client *Client) {
	h.mu.Lock()
	room, exists := h.Rooms[client.RoomID]
	if !exists {
		room = &Room{ID: client.RoomID, Clients: make(map[string]*Client)}
		h.Rooms[client.RoomID] = room
	}
	h.mu.Unlock()

	room.mu.Lock()
	room.Clients[client.ClientID] = client
	room.mu.Unlock()

	log.Printf("[Hub] Client %s joined room %s (total: %d)", client.ClientID, client.RoomID, len(room.Clients))
}

// removeClient 从房间移除客户端
func (h *Hub) removeClient(client *Client) {
	h.mu.RLock()
	room, exists := h.Rooms[client.RoomID]
	h.mu.RUnlock()
	if !exists {
		return
	}

	room.mu.Lock()
	if _, ok := room.Clients[client.ClientID]; ok {
		delete(room.Clients, client.ClientID)
		client.Close()
	}
	isEmpty := len(room.Clients) == 0
	room.mu.Unlock()

	// 如果房间为空，删除
	if isEmpty {
		h.mu.Lock()
		delete(h.Rooms, client.RoomID)
		h.mu.Unlock()
		log.Printf("[Hub] Room %s empty, removed", client.RoomID)
	} else {
		// 通知其他用户
		h.broadcastUserLeave(room, client)
		log.Printf("[Hub] Client %s left room %s (remaining: %d)", client.ClientID, client.RoomID, len(room.Clients))
	}
}

// broadcast 发送消息到房间内所有客户端
func (h *Hub) broadcast(msg *BroadcastMessage) {
	h.mu.RLock()
	room, exists := h.Rooms[msg.RoomID]
	h.mu.RUnlock()
	if !exists {
		return
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, client := range room.Clients {
		if client.ClientID == msg.Except {
			continue
		}
		select {
		case client.Send <- msg.Message:
		default:
			log.Printf("[Hub] Client %s send buffer full, dropping message", client.ClientID)
		}
	}
}

// broadcastUserLeave 广播用户离开事件
func (h *Hub) broadcastUserLeave(room *Room, leaving *Client) {
	msg := Message{
		Type:     MsgUserLeave,
		DocID:    room.ID,
		ClientID: leaving.ClientID,
		Username: leaving.Username,
	}
	data, _ := json.Marshal(msg)

	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, client := range room.Clients {
		select {
		case client.Send <- data:
		default:
		}
	}
}

// BroadcastOp 广播操作消息
func (h *Hub) BroadcastOp(roomID string, senderID string, opMsg Message) {
	data, _ := json.Marshal(opMsg)
	h.Broadcast <- &BroadcastMessage{
		RoomID:  roomID,
		Message: data,
		Except:  senderID,
	}
}

// BroadcastCursors 广播光标位置
func (h *Hub) BroadcastCursors(roomID string, senderID string, cursorMsg Message) {
	data, _ := json.Marshal(cursorMsg)
	h.Broadcast <- &BroadcastMessage{
		RoomID:  roomID,
		Message: data,
		Except:  senderID,
	}
}

// BroadcastToAll 向房间内所有客户端广播（包括发送者）。
// 用于文档删除等需要每个在线用户都收到的系统通知。
func (h *Hub) BroadcastToAll(roomID string, m Message) {
	data, _ := json.Marshal(m)
	h.Broadcast <- &BroadcastMessage{
		RoomID:  roomID,
		Message: data,
	}
}

// ForceCloseRoom 强制关闭某个文档房间：断开所有在线用户的连接并移除房间。
// 调用方应在广播 doc_deleted 通知后稍作延迟再调用，给消息留出投递时间。
func (h *Hub) ForceCloseRoom(roomID string) {
	h.mu.Lock()
	room, exists := h.Rooms[roomID]
	if exists {
		delete(h.Rooms, roomID)
	}
	h.mu.Unlock()
	if !exists {
		return
	}

	// 持有房间锁时关闭客户端，与 broadcast 的发送操作互斥，
	// 避免向已关闭的 Send 通道写入导致panic
	room.mu.Lock()
	n := 0
	for _, c := range room.Clients {
		c.Close()
		n++
	}
	room.Clients = make(map[string]*Client)
	room.mu.Unlock()

	log.Printf("[Hub] Room %s force-closed, %d client(s) disconnected", roomID, n)
}

// GetRoomUsers 获取房间内所有用户信息
func (h *Hub) GetRoomUsers(roomID string) []UserInfo {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return nil
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	users := make([]UserInfo, 0, len(room.Clients))
	for _, c := range room.Clients {
		users = append(users, UserInfo{
			ClientID: c.ClientID,
			Username: c.Username,
			Color:    c.Color,
		})
	}
	return users
}

// GetRoomCursors 获取房间内所有光标位置
func (h *Hub) GetRoomCursors(roomID string) []Cursor {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return nil
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	cursors := make([]Cursor, 0, len(room.Clients))
	for _, c := range room.Clients {
		cursors = append(cursors, Cursor{
			ClientID: c.ClientID,
			Username: c.Username,
			Position: c.Position,
			Color:    c.Color,
		})
	}
	return cursors
}

// IsRoomEmpty 检查房间是否为空
func (h *Hub) IsRoomEmpty(roomID string) bool {
	h.mu.RLock()
	room, exists := h.Rooms[roomID]
	h.mu.RUnlock()
	if !exists {
		return true
	}
	room.mu.RLock()
	defer room.mu.RUnlock()
	return len(room.Clients) == 0
}
