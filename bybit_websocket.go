package bybit_connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	tstamp = "02/01 15:04:05.000"
)

var (
	pMux sync.Mutex
	rMux sync.RWMutex
)

type MessageHandler func(message string) error

func (b *WebSocket) ReConnect(delay int) {
	fmt.Println(time.Now().Format(tstamp), "Cleaning by disconnect ", b.subtopic)
	b.Disconnect()
	b.wg.Wait()
	fmt.Println(time.Now().Format(tstamp), "Attempting to reconnect ", b.subtopic)
	con := b.Connect() // Example, adjust parameters as needed
	if con == nil {
		fmt.Println(time.Now().Format(tstamp), "Reconnection failed:")
		b.isConnected = false
		if delay <= 120 {
			delay *= 2
		}
		time.Sleep(time.Duration(delay) * time.Second) //rate limiting retry
		go b.ReConnect(delay)
	} else {
		b.isConnected = true
		//go b.handleIncomingMessages() // Restart message handling
	}
}

func (b *WebSocket) handleIncomingMessages() {
	b.wg.Add(1)
	defer b.wg.Done()
	fmt.Println(time.Now().Format(tstamp), "Setup handle incoming message ", b.subtopic)
	for {
		_, message, err := b.conn.ReadMessage()
		if err != nil {
			fmt.Println(time.Now().Format(tstamp), "Error reading:", err)
			b.isConnected = false
			return
		}
		rMux.Lock()
		b.lastReceive = time.Now()
		rMux.Unlock()

		if b.onMessage != nil {
			if !strings.Contains(string(message), `"op":"pong"`) {
				err := b.onMessage(string(message))
				if err != nil {
					fmt.Println(time.Now().Format(tstamp), "Error handling message:", err)
					return
				}
			}
		}
	}
}

func (b *WebSocket) monitorConnection() {
	b.wg.Add(1)
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second * 5) // Check every 5 seconds
	defer ticker.Stop()
	fmt.Println(time.Now().Format(tstamp), "Setup connection monitoring ", b.subtopic)
	rMux.Lock()
	b.lastReceive = time.Now()
	rMux.Unlock()
	for {
		select {
		case <-ticker.C:
			if !b.isConnected && b.ctx.Err() == nil { // Check if disconnected and context not done
				go b.ReConnect(1)
			}
			rMux.RLock()
			if time.Since(b.lastReceive) > time.Duration(b.pingInterval)*time.Second {
				fmt.Println(time.Now().Format(tstamp), "No data received within ping interval ", b.subtopic)
				go b.ReConnect(1)
			}
			rMux.RUnlock()
		case <-b.ctx.Done():
			fmt.Println(time.Now().Format(tstamp), "Exiting conn monitoring ", b.subtopic)
			return // Stop the routine if context is done
		}
	}
}

func (b *WebSocket) SetMessageHandler(handler MessageHandler) {
	b.onMessage = handler
}

type WebSocket struct {
	conn         *websocket.Conn
	url          string
	apiKey       string
	apiSecret    string
	maxAliveTime string
	lastReceive  time.Time
	pingInterval int
	onMessage    MessageHandler
	ctx          context.Context
	cancel       context.CancelFunc
	subtopic     []string
	isConnected  bool
	wg           sync.WaitGroup
}

type WebsocketOption func(*WebSocket)

func WithPingInterval(pingInterval int) WebsocketOption {
	return func(c *WebSocket) {
		c.pingInterval = pingInterval
	}
}

func WithMaxAliveTime(maxAliveTime string) WebsocketOption {
	return func(c *WebSocket) {
		c.maxAliveTime = maxAliveTime
	}
}

func NewBybitPrivateWebSocket(url, apiKey, apiSecret string, handler MessageHandler, options ...WebsocketOption) *WebSocket {
	c := &WebSocket{
		url:          url,
		apiKey:       apiKey,
		apiSecret:    apiSecret,
		maxAliveTime: "",
		pingInterval: 20,
		onMessage:    handler,
	}

	// Apply the provided options
	for _, opt := range options {
		opt(c)
	}

	return c
}

func NewBybitPublicWebSocket(url string, handler MessageHandler) *WebSocket {
	c := &WebSocket{
		url:          url,
		pingInterval: 20, // default is 20 seconds
		onMessage:    handler,
	}

	return c
}

func (b *WebSocket) Connect() *WebSocket {
	var err error
	wssUrl := b.url
	if b.maxAliveTime != "" {
		wssUrl += "?max_alive_time=" + b.maxAliveTime
	}

	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  45 * time.Second,
		ReadBufferSize:    16384,
		WriteBufferSize:   4096,
		EnableCompression: true,
	}

	b.conn, _, err = dialer.Dial(wssUrl, nil)
	if err != nil {
		fmt.Printf("%s Failed Dial: %v", time.Now().Format(tstamp), err)
		return nil
	}

	if b.requiresAuthentication() {
		if err = b.sendAuth(); err != nil {
			fmt.Println(time.Now().Format(tstamp), "Failed Connection:", fmt.Sprintf("%v", err))
			return nil
		}
	}
	b.isConnected = true

	go b.handleIncomingMessages()
	go b.monitorConnection()

	b.ctx, b.cancel = context.WithCancel(context.Background())
	go ping(b)

	if len(b.subtopic) > 0 {
		_, err := b.SendSubscription(b.subtopic)
		if err != nil {
			return nil
		}
	}

	return b
}

func (b *WebSocket) SendSubscription(args []string) (*WebSocket, error) {
	b.subtopic = args
	reqID := uuid.New().String()
	subMessage := map[string]interface{}{
		"req_id": reqID,
		"op":     "subscribe",
		"args":   args,
	}
	fmt.Println(time.Now().Format(tstamp), "subscribe msg:", fmt.Sprintf("%v", subMessage["args"]))
	if err := b.sendAsJson(subMessage); err != nil {
		fmt.Println(time.Now().Format(tstamp), "Failed to send subscription:", err)
		return b, err
	}
	fmt.Println(time.Now().Format(tstamp), "Subscription sent successfully.")
	return b, nil
}

// sendRequest sends a custom request over the WebSocket connection.
func (b *WebSocket) sendRequest(op string, args map[string]interface{}, headers map[string]string) error {
	reqID := uuid.New().String()
	request := map[string]interface{}{
		"reqId":  reqID,
		"header": headers,
		"op":     op,
		"args":   []interface{}{args},
	}
	fmt.Println("request headers:", fmt.Sprintf("%v", request["header"]))
	fmt.Println("request op channel:", fmt.Sprintf("%v", request["op"]))
	fmt.Println("request msg:", fmt.Sprintf("%v", request["args"]))
	return b.sendAsJson(request)
}

func ping(b *WebSocket) {
	b.wg.Add(1)
	defer b.wg.Done()
	fmt.Println(time.Now().Format(tstamp), "Setup ping handler ", b.subtopic)
	if b.pingInterval <= 0 {
		fmt.Println(time.Now().Format(tstamp), "Ping interval is set to a non-positive value.")
		return
	}

	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if b.isConnected {
				currentTime := time.Now().Unix()
				pingMessage := map[string]string{
					"op":     "ping",
					"req_id": fmt.Sprintf("%d", currentTime),
				}
				jsonPingMessage, err := json.Marshal(pingMessage)
				if err != nil {
					fmt.Println("Failed to marshal ping message:", err)
					goto exittick
				}
				if err := b.send(string(jsonPingMessage)); err != nil {
					fmt.Println("Failed to send ping:", err)
				} else {
					//fmt.Println("Ping sent with UTC time:", currentTime)
				}
			exittick:
			} else {
				fmt.Println(time.Now().Format(tstamp), "Ping suspended when disconnected ", b.subtopic)
			}

		case <-b.ctx.Done():
			fmt.Println(time.Now().Format(tstamp), "Ping context closed, stopping ping ", b.subtopic)
			return
		}
	}
}

func (b *WebSocket) Disconnect() error {
	if b != nil {
		if b.cancel != nil {
			b.cancel()
		}
		b.isConnected = false
		if b.conn != nil {
			return b.conn.Close()
		}
		return nil
	}
	return nil
}

func (b *WebSocket) requiresAuthentication() bool {
	return b.url == WEBSOCKET_PRIVATE_MAINNET ||
		b.url == WEBSOCKET_PRIVATE_TESTNET || b.url == WEBSOCKET_TRADE_MAINNET || b.url == WEBSOCKET_TRADE_TESTNET || b.url == WEBSOCKET_TRADE_DEMO || b.url == WEBSOCKET_PRIVATE_DEMO
	// v3 offline
	/*
		b.url == V3_CONTRACT_PRIVATE ||
			b.url == V3_UNIFIED_PRIVATE ||
			b.url == V3_SPOT_PRIVATE
	*/
}

func (b *WebSocket) sendAuth() error {
	// Get current Unix time in milliseconds
	expires := time.Now().UnixNano()/1e6 + 10000
	val := fmt.Sprintf("GET/realtime%d", expires)

	h := hmac.New(sha256.New, []byte(b.apiSecret))
	h.Write([]byte(val))

	// Convert to hexadecimal instead of base64
	signature := hex.EncodeToString(h.Sum(nil))
	fmt.Println("signature generated : " + signature)

	authMessage := map[string]interface{}{
		"req_id": uuid.New(),
		"op":     "auth",
		"args":   []interface{}{b.apiKey, expires, signature},
	}
	fmt.Println("auth args:", fmt.Sprintf("%v", authMessage["args"]))
	return b.sendAsJson(authMessage)
}

func (b *WebSocket) sendAsJson(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.send(string(data))
}

func (b *WebSocket) send(message string) error {
	pMux.Lock()
	defer pMux.Unlock()
	return b.conn.WriteMessage(websocket.TextMessage, []byte(message))
}
