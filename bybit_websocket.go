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

	// WebSocket Configuration Constants
	DefaultPingInterval      = 20    // seconds
	DefaultMonitorInterval   = 5     // seconds
	DefaultDisconnectTimeout = 15    // seconds
	DefaultHandshakeTimeout  = 45    // seconds
	MaxReconnectionDelay     = 120   // seconds
	DefaultReadBufferSize    = 16384 // bytes
	DefaultWriteBufferSize   = 4096  // bytes

	// Connection health constants
	DefaultReadTimeout  = 60          // seconds - timeout for read operations
	DefaultWriteTimeout = 10          // seconds - timeout for write operations
	MaxMessageSize      = 1024 * 1024 // 1MB - maximum message size
	PongTimeout         = 60          // seconds - timeout waiting for pong response
)

type MessageHandler func(message string) error

func (b *WebSocket) ReConnect(delay int) {
	// Prevent multiple simultaneous reconnection attempts
	if !b.setReconnecting(true) {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Reconnection already in progress ", b.subtopic)
		}
		return
	}
	defer b.setReconnecting(false)

	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Cleaning by disconnect ", b.subtopic)
	}
	// Best-effort disconnect; log error in debug mode
	if err := b.Disconnect(); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "Disconnect error:", err)
	}
	b.wg.Wait()

	// Keep trying to reconnect with exponential backoff
	currentDelay := delay
	for {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Attempting to reconnect ", b.subtopic)
		}
		con := b.Connect()
		if con != nil {
			b.setConnected(true)
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Reconnection successful ", b.subtopic)
			}
			return
		}

		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Reconnection failed, retrying in", currentDelay, "seconds")
		}
		b.setConnected(false)

		// Use interruptible sleep that responds to context cancellation
		timer := time.NewTimer(time.Duration(currentDelay) * time.Second)
		select {
		case <-timer.C:
			// Normal delay completed
		case <-b.ctx.Done():
			timer.Stop()
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Context cancelled during sleep, stopping reconnection")
			}
			return
		}

		if currentDelay <= MaxReconnectionDelay {
			currentDelay *= 2
		}
	}
}

func (b *WebSocket) handleIncomingMessages() {
	b.wg.Add(1)
	defer b.wg.Done()
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Setup handle incoming message ", b.subtopic)
	}
	for {
		// Check if connection is nil before attempting to read (with mutex protection)
		b.connMux.RLock()
		conn := b.conn
		b.connMux.RUnlock()

		if conn == nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Connection is nil, exiting message handler")
			}
			b.setConnected(false)
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Error reading:", err)
			}
			b.setConnected(false)
			// Only trigger reconnection if context is not cancelled
			if b.ctx.Err() == nil {
				go b.ReConnect(1)
			}
			return
		}

		// Reset read deadline for next message
		if err := conn.SetReadDeadline(time.Now().Add(DefaultReadTimeout * time.Second)); err != nil && b.debug {
			fmt.Println(time.Now().Format(tstamp), "SetReadDeadline error:", err)
		}

		b.receiveMux.Lock()
		b.lastReceive = time.Now()
		b.receiveMux.Unlock()

		if b.onMessage != nil {
			err := b.onMessage(string(message))
			if err != nil {
				fmt.Println(time.Now().Format(tstamp), "Error handling message:", err)
				// Don't exit on message handler error, just log it
				// The application should decide whether to disconnect
				continue
			}
		}
	}
}

func (b *WebSocket) monitorConnection() {
	b.wg.Add(1)
	defer b.wg.Done()
	ticker := time.NewTicker(DefaultMonitorInterval * time.Second)
	defer ticker.Stop()
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Setup connection monitoring ", b.subtopic)
	}
	b.receiveMux.Lock()
	b.lastReceive = time.Now()
	b.lastPong = time.Now() // Initialize pong time
	b.receiveMux.Unlock()
	for {
		select {
		case <-ticker.C:
			if !b.isConnectedSafe() && b.ctx.Err() == nil { // Check if disconnected and context not done
				go b.ReConnect(1)
			}
			b.receiveMux.RLock()
			lastReceive := b.lastReceive
			b.receiveMux.RUnlock()

			if time.Since(lastReceive) > time.Duration(b.pingInterval)*time.Second {
				if b.debug {
					fmt.Println(time.Now().Format(tstamp), "No data received within ping interval ", b.subtopic)
				}
				// Only trigger reconnection if context is still active and not already reconnecting
				if b.ctx.Err() == nil {
					go b.ReConnect(1)
				}
			}
		case <-b.ctx.Done():
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Exiting conn monitoring ", b.subtopic)
			}
			return // Stop the routine if context is done
		}
	}
}

func (b *WebSocket) SetMessageHandler(handler MessageHandler) {
	b.onMessage = handler
}

// setConnected safely sets the connection state
func (b *WebSocket) setConnected(connected bool) {
	b.connMux.Lock()
	defer b.connMux.Unlock()
	b.isConnected = connected
}

// isConnectedSafe safely reads the connection state
func (b *WebSocket) isConnectedSafe() bool {
	b.connMux.RLock()
	defer b.connMux.RUnlock()
	return b.isConnected
}

// setReconnecting safely sets the reconnection state
func (b *WebSocket) setReconnecting(reconnecting bool) bool {
	b.reconnectMux.Lock()
	defer b.reconnectMux.Unlock()
	if reconnecting && b.isReconnecting {
		return false // Already reconnecting
	}
	b.isReconnecting = reconnecting
	return true
}

type WebSocket struct {
	conn           *websocket.Conn
	url            string
	apiKey         string
	apiSecret      string
	maxAliveTime   string
	lastReceive    time.Time
	lastPong       time.Time // Track last pong response for connection health
	pingInterval   int
	onMessage      MessageHandler
	ctx            context.Context
	cancel         context.CancelFunc
	subtopic       []string
	isConnected    bool
	isReconnecting bool
	debug          bool
	wg             sync.WaitGroup
	sendMux        sync.Mutex   // Protects WebSocket send operations
	receiveMux     sync.RWMutex // Protects lastReceive time
	connMux        sync.RWMutex // Protects isConnected state and connection
	reconnectMux   sync.Mutex   // Protects reconnection state
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
		pingInterval: DefaultPingInterval,
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
		pingInterval: DefaultPingInterval,
		onMessage:    handler,
	}

	return c
}

func (b *WebSocket) SetDebug(dbg bool) {
	b.debug = dbg
}

func (b *WebSocket) Connect() *WebSocket {
	var err error
	wssUrl := b.url
	if b.maxAliveTime != "" {
		wssUrl += "?max_alive_time=" + b.maxAliveTime
	}

	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  DefaultHandshakeTimeout * time.Second,
		ReadBufferSize:    DefaultReadBufferSize,
		WriteBufferSize:   DefaultWriteBufferSize,
		EnableCompression: true,
	}

	conn, _, err := dialer.Dial(wssUrl, nil)
	if err != nil {
		fmt.Printf("%s Failed Dial: %v", time.Now().Format(tstamp), err)
		return nil
	}

	// Configure connection-level settings for persistent connections
	conn.SetReadLimit(MaxMessageSize)
	if err := conn.SetReadDeadline(time.Now().Add(DefaultReadTimeout * time.Second)); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "SetReadDeadline error:", err)
	}

	// Set up pong handler for proper ping/pong cycle
	conn.SetPongHandler(func(appData string) error {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Received pong from server")
		}
		// Update last pong time for connection health monitoring
		b.receiveMux.Lock()
		b.lastPong = time.Now()
		b.receiveMux.Unlock()

		// Reset read deadline when we receive a pong
		if err := conn.SetReadDeadline(time.Now().Add(DefaultReadTimeout * time.Second)); err != nil && b.debug {
			fmt.Println(time.Now().Format(tstamp), "SetReadDeadline error:", err)
		}
		return nil
	})

	// Safely assign connection with mutex protection
	b.connMux.Lock()
	b.conn = conn
	b.connMux.Unlock()

	// Set connected status BEFORE auth/send operations to avoid race condition
	b.setConnected(true)

	if b.requiresAuthentication() {
		if err = b.sendAuth(); err != nil {
			fmt.Println(time.Now().Format(tstamp), "Failed Connection:", fmt.Sprintf("%v", err))

			// Close connection on authentication failure (with mutex protection)
			b.setConnected(false)
			b.connMux.Lock()
			if b.conn != nil {
				b.conn.Close()
			}
			b.connMux.Unlock()

			return nil
		}
	}

	// Cancel old context before creating new one to prevent resource leaks
	if b.cancel != nil {
		b.cancel()
	}

	// Create context before starting goroutines
	b.ctx, b.cancel = context.WithCancel(context.Background())

	go b.handleIncomingMessages()
	go b.monitorConnection()
	go ping(b)

	if len(b.subtopic) > 0 {
		_, err := b.SendSubscription(b.subtopic)
		if err != nil {
			// Cleanup resources if subscription fails
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Subscription failed, cleaning up resources:", err)
			}

			// Cancel context to stop goroutines
			if b.cancel != nil {
				b.cancel()
			}

			// Close connection (with mutex protection)
			b.connMux.Lock()
			if b.conn != nil {
				b.conn.Close()
			}
			b.connMux.Unlock()

			b.setConnected(false)
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
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "subscribe msg:", fmt.Sprintf("%v", subMessage["args"]))
	}
	if err := b.sendAsJson(subMessage); err != nil {
		fmt.Println(time.Now().Format(tstamp), "Failed to send subscription:", err)
		return b, err
	}
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Subscription sent successfully.")
	}
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
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Setup ping handler ", b.subtopic)
	}
	if b.pingInterval <= 0 {
		fmt.Println(time.Now().Format(tstamp), "Ping interval is set to a non-positive value.")
		return
	}

	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if b.isConnectedSafe() {
				// Optimized ping message creation - avoid map allocation and JSON marshaling
				currentTime := time.Now().Unix()
				pingMessage := fmt.Sprintf(`{"op":"ping","req_id":"%d"}`, currentTime)

				if err := b.send(pingMessage); err != nil {
					// Force immediate reconnection on connection-related errors
					if strings.Contains(err.Error(), "use of closed network connection") ||
						strings.Contains(err.Error(), "connection reset by peer") ||
						strings.Contains(err.Error(), "broken pipe") ||
						strings.Contains(err.Error(), "connection is nil") ||
						strings.Contains(strings.ToLower(err.Error()), "not connected") {
						if b.debug {
							fmt.Println(time.Now().Format(tstamp), "Ping detected broken connection, triggering reconnection")
						}
						b.setConnected(false)
						if b.ctx.Err() == nil {
							go b.ReConnect(1)
						}
					} else {
						fmt.Println("Failed to send ping:", err)
					}
				}
			} else {
				if b.debug {
					fmt.Println(time.Now().Format(tstamp), "Ping suspended when disconnected ", b.subtopic)
				}
			}

		case <-b.ctx.Done():
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Ping context closed, stopping ping ", b.subtopic)
			}
			return
		}
	}
}

func (b *WebSocket) Disconnect() error {
	if b != nil {
		// Cancel context first to stop all goroutines
		if b.cancel != nil {
			b.cancel()
		}
		b.setConnected(false)

		// Close the connection (with mutex protection)
		var err error
		b.connMux.Lock()
		if b.conn != nil {
			err = b.conn.Close()
		}
		b.connMux.Unlock()

		// Wait for all goroutines to finish with a timeout
		done := make(chan struct{})
		go func() {
			defer close(done)
			b.wg.Wait()
		}()

		select {
		case <-done:
			// All goroutines finished gracefully
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "All goroutines finished gracefully")
			}
		case <-time.After(DefaultDisconnectTimeout * time.Second):
			// Timeout waiting for goroutines - force cleanup
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Timeout waiting for goroutines, forcing cleanup")
			}
			// Force close connection if still open (with mutex protection)
			b.connMux.Lock()
			if b.conn != nil {
				b.conn.Close()
				b.conn = nil
			}
			b.connMux.Unlock()
		}

		return err
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
	if b.debug {
		fmt.Println("signature generated : " + signature)
	}

	authMessage := map[string]interface{}{
		"req_id": uuid.New().String(),
		"op":     "auth",
		"args":   []interface{}{b.apiKey, expires, signature},
	}
	if b.debug {
		fmt.Println("auth args:", fmt.Sprintf("%v", authMessage["args"]))
	}
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
	b.sendMux.Lock()
	defer b.sendMux.Unlock()

	// Do not send if context already closed
	if b.ctx != nil && b.ctx.Err() != nil {
		return fmt.Errorf("context closed")
	}

	// Ensure we are connected
	if !b.isConnectedSafe() {
		return fmt.Errorf("websocket not connected")
	}

	// Check if connection is nil before attempting to send (with mutex protection)
	b.connMux.RLock()
	conn := b.conn
	b.connMux.RUnlock()

	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Set write deadline for persistent connection reliability
	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout * time.Second)); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "SetWriteDeadline error:", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
		// On low-level network errors, flip connected state and trigger reconnection
		msg := err.Error()
		if strings.Contains(msg, "use of closed network connection") ||
			strings.Contains(msg, "connection reset by peer") ||
			strings.Contains(msg, "broken pipe") ||
			strings.Contains(strings.ToLower(msg), "write: connection") {
			b.setConnected(false)
			if b.ctx != nil && b.ctx.Err() == nil {
				go b.ReConnect(1)
			}
		}
		return err
	}
	return nil
}
