package bybit_connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
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

// isConnectionError checks if an error indicates a broken connection
// Uses pre-compiled checks to avoid repeated string operations
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	// Treat network timeouts as connection errors so we reconnect on stalls.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// Close frames/errors indicate the connection is no longer usable.
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return true
	}
	msg := err.Error()
	// Check common connection errors
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection is nil") ||
		strings.Contains(strings.ToLower(msg), "not connected") ||
		strings.Contains(strings.ToLower(msg), "write: connection") ||
		strings.Contains(strings.ToLower(msg), "i/o timeout") ||
		strings.Contains(strings.ToLower(msg), "timeout")
}

// triggerReconnect initiates reconnection in a separate goroutine
// This is non-blocking and deduplicates multiple simultaneous requests using isReconnecting flag
// Safe to call from any goroutine without holding locks
func (b *WebSocket) triggerReconnect() {
	// Check shutdown state first
	b.connMux.RLock()
	shuttingDown := b.isShuttingDown
	b.connMux.RUnlock()

	if shuttingDown {
		return // Don't reconnect during shutdown
	}

	// Cheap pre-check to avoid spawning extra goroutines when already reconnecting.
	b.reconnectMux.Lock()
	alreadyReconnecting := b.isReconnecting
	b.reconnectMux.Unlock()
	if alreadyReconnecting {
		return
	}

	// Spawn reconnection in separate goroutine (not tracked by wg to avoid deadlock)
	// The setReconnecting flag prevents multiple simultaneous reconnection attempts
	go b.ReConnect(1)
}

func (b *WebSocket) ReConnect(delay int) {
	// Prevent multiple simultaneous reconnection attempts (acquire highest level lock first)
	if !b.setReconnecting(true) {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Reconnection already in progress ", b.subtopic)
		}
		return
	}
	defer b.setReconnecting(false)

	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Starting reconnection process ", b.subtopic)
	}

	// Step 1: Clean up existing connection (lightweight cleanup, don't wait for goroutines here)
	b.cleanupConnection()

	// Step 2: Wait for existing goroutines to finish (they will exit due to context cancellation)
	// Use timeout to avoid infinite wait
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Old goroutines finished ", b.subtopic)
		}
	case <-time.After(5 * time.Second):
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Timeout waiting for old goroutines, proceeding anyway ", b.subtopic)
		}
	}

	// Step 3: Check if we should abort (user called Disconnect during cleanup)
	b.connMux.RLock()
	shuttingDown := b.isShuttingDown
	b.connMux.RUnlock()
	if shuttingDown {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Shutdown requested, aborting reconnection ", b.subtopic)
		}
		return
	}

	// Step 4: Keep trying to reconnect with exponential backoff
	currentDelay := delay
	for {
		// Check shutdown state before each attempt
		b.connMux.RLock()
		shuttingDown = b.isShuttingDown
		b.connMux.RUnlock()
		if shuttingDown {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Shutdown requested, stopping reconnection ", b.subtopic)
			}
			return
		}

		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Attempting to reconnect ", b.subtopic)
		}

		con := b.Connect()
		if con != nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Reconnection successful ", b.subtopic)
			}
			return
		}

		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Reconnection failed, retrying in", currentDelay, "seconds")
		}

		// Interruptible sleep with shutdown check
		// Poll for shutdown every 500ms during the delay
		sleepRemaining := time.Duration(currentDelay) * time.Second
		sleepInterval := 500 * time.Millisecond
		aborted := false
		for sleepRemaining > 0 {
			// Check shutdown before sleeping
			b.connMux.RLock()
			shuttingDown = b.isShuttingDown
			b.connMux.RUnlock()
			if shuttingDown {
				if b.debug {
					fmt.Println(time.Now().Format(tstamp), "Shutdown detected during reconnect delay, aborting")
				}
				aborted = true
				break
			}

			// Sleep for the smaller of remaining time or interval
			sleepTime := min(sleepRemaining, sleepInterval)
			time.Sleep(sleepTime)
			sleepRemaining -= sleepTime
		}
		if aborted {
			return
		}

		// Exponential backoff with cap
		if currentDelay < MaxReconnectionDelay {
			currentDelay *= 2
			if currentDelay > MaxReconnectionDelay {
				currentDelay = MaxReconnectionDelay
			}
		}
	}
}

// cleanupConnection closes the connection and cancels context without waiting for goroutines
// This is used internally by ReConnect to avoid deadlock
func (b *WebSocket) cleanupConnection() {
	b.connMux.Lock()
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil // Clear cancel to signal cleanup is complete
	}
	b.ctx = nil
	b.handlerCh = nil
	b.isConnected = false
	if b.conn != nil {
		b.connID++ // Invalidate in-flight goroutines tied to the old connection
		b.conn.Close()
		b.conn = nil
	}
	b.connMux.Unlock()
}

func (b *WebSocket) handleIncomingMessages(ctx context.Context, conn *websocket.Conn, id uint64) {
	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Setup handle incoming message ", b.subtopic)
	}
	for {
		if ctx != nil && ctx.Err() != nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Message handler context closed, exiting")
			}
			return
		}
		if conn == nil {
			return
		}
		// If a newer connection exists, exit quietly.
		if !b.isConnCurrent(id) {
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Error reading:", err)
			}
			// Only the current connection should flip state / trigger reconnect.
			if b.isConnCurrent(id) {
				b.setConnected(false)
				b.triggerReconnect()
			}
			return
		}

		// If the connection changed while ReadMessage unblocked, drop the message.
		if !b.isConnCurrent(id) {
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
			// Enqueue to handler worker if configured, otherwise run inline.
			b.connMux.RLock()
			handlerCh := b.handlerCh
			b.connMux.RUnlock()
			if handlerCh != nil {
				select {
				case handlerCh <- string(message):
					// queued
				default:
					if b.debug {
						fmt.Println(time.Now().Format(tstamp), "Handler queue full, dropping message")
					}
				}
			} else {
				handlerErr := func() (err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic in message handler: %v", r)
						}
					}()
					return b.onMessage(string(message))
				}()
				if handlerErr != nil {
					fmt.Println(time.Now().Format(tstamp), "Error handling message:", handlerErr)
					// Don't exit on message handler error, just log it
					// The application should decide whether to disconnect
					continue
				}
			}
		}
	}
}

func (b *WebSocket) monitorConnection(ctx context.Context, id uint64) {
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
			// If a newer connection exists, stop monitoring this one.
			if !b.isConnCurrent(id) {
				return
			}
			// Single health check: combine connection state and timeout check
			b.connMux.RLock()
			connected := b.isConnected
			b.connMux.RUnlock()

			if !connected {
				if b.isConnCurrent(id) {
					b.triggerReconnect()
				}
				continue
			}

			// Check receive timeout
			b.receiveMux.RLock()
			lastReceive := b.lastReceive
			lastPong := b.lastPong
			b.receiveMux.RUnlock()

			// Use last activity (either data received or pong) to avoid false stale detection
			lastActivity := lastReceive
			if lastPong.After(lastActivity) {
				lastActivity = lastPong
			}

			// Only trigger reconnection if no data received for 2x pingInterval
			// This gives enough time for at least one ping-pong cycle
			staleTimeout := time.Duration(b.pingInterval*2) * time.Second
			if time.Since(lastActivity) > staleTimeout {
				if b.debug {
					fmt.Println(time.Now().Format(tstamp), "No data received within stale timeout ", b.subtopic)
				}
				if b.isConnCurrent(id) {
					b.setConnected(false)
					b.triggerReconnect()
				}
			}
		case <-ctx.Done():
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
// Must not be called while holding other locks to avoid deadlock
func (b *WebSocket) setConnected(connected bool) {
	b.connMux.Lock()
	b.isConnected = connected
	b.connMux.Unlock()
}

// isConnectedSafe safely reads the connection state
// Can be called while holding reconnectMux (follows lock hierarchy)
func (b *WebSocket) isConnectedSafe() bool {
	b.connMux.RLock()
	connected := b.isConnected
	b.connMux.RUnlock()
	return connected
}

// GetLastActivityTime returns the most recent activity timestamp (lastReceive or lastPong) in Unix nanoseconds
// This can be used to monitor connection health from external code
func (b *WebSocket) GetLastActivityTime() int64 {
	b.receiveMux.RLock()
	lastPong := b.lastPong
	lastReceive := b.lastReceive
	b.receiveMux.RUnlock()

	// Return the more recent timestamp as Unix nanoseconds
	if lastPong.After(lastReceive) {
		return lastPong.UnixNano()
	}
	return lastReceive.UnixNano()
}

// setReconnecting safely sets the reconnection state
// This is the highest level lock - never acquire other locks while holding this
func (b *WebSocket) setReconnecting(reconnecting bool) bool {
	b.reconnectMux.Lock()
	if reconnecting && b.isReconnecting {
		b.reconnectMux.Unlock()
		return false // Already reconnecting
	}
	b.isReconnecting = reconnecting
	b.reconnectMux.Unlock()
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
	handlerQueueSz int
	handlerCh      chan string
	onMessage      MessageHandler
	ctx            context.Context
	cancel         context.CancelFunc
	subtopic       []string
	isConnected    bool
	isReconnecting bool
	isShuttingDown bool // Prevents new operations during shutdown
	connID         uint64
	debug          bool
	wg             sync.WaitGroup
	// Mutex lock hierarchy (acquire in this order to prevent deadlocks):
	// 1. reconnectMux (highest level - controls reconnection state)
	// 2. connMux (protects connection state and context)
	// 3. sendMux/receiveMux (lowest level - protects I/O operations)
	reconnectMux sync.Mutex   // Level 1: Protects reconnection state
	connMux      sync.RWMutex // Level 2: Protects isConnected, isShuttingDown, conn, ctx, cancel
	dialMux      sync.Mutex   // Serializes Connect() to avoid concurrent dials
	sendMux      sync.Mutex   // Level 3: Protects WebSocket send operations
	receiveMux   sync.RWMutex // Level 3: Protects lastReceive and lastPong times
}

// isConnCurrent returns true if id still matches the active connection.
// This prevents old goroutines from flipping state or triggering reconnect after a newer connection is established.
func (b *WebSocket) isConnCurrent(id uint64) bool {
	b.connMux.RLock()
	current := b.connID == id && b.conn != nil
	b.connMux.RUnlock()
	return current
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

// WithHandlerQueueSize sets the buffered queue size for message handling.
// When the queue is full, messages are dropped (and logged if debug is on).
func WithHandlerQueueSize(size int) WebsocketOption {
	return func(c *WebSocket) {
		c.handlerQueueSz = size
	}
}

func NewBybitPrivateWebSocket(url, apiKey, apiSecret string, handler MessageHandler, options ...WebsocketOption) *WebSocket {
	c := &WebSocket{
		url:            url,
		apiKey:         apiKey,
		apiSecret:      apiSecret,
		maxAliveTime:   "",
		pingInterval:   DefaultPingInterval,
		handlerQueueSz: 100,
		onMessage:      handler,
	}

	// Apply the provided options
	for _, opt := range options {
		opt(c)
	}

	return c
}

func NewBybitPublicWebSocket(url string, handler MessageHandler, options ...WebsocketOption) *WebSocket {
	c := &WebSocket{
		url:            url,
		pingInterval:   DefaultPingInterval,
		handlerQueueSz: 100,
		onMessage:      handler,
	}

	// Apply the provided options
	for _, opt := range options {
		opt(c)
	}

	return c
}

func (b *WebSocket) SetDebug(dbg bool) {
	b.debug = dbg
}

func (b *WebSocket) Connect() *WebSocket {
	b.dialMux.Lock()
	defer b.dialMux.Unlock()

	var err error
	wssUrl := b.url
	if b.maxAliveTime != "" {
		wssUrl += "?max_alive_time=" + b.maxAliveTime
	}

	// Fully clean up any existing connection first.
	// This prevents leaked parallel connections when Connect() is called manually.
	b.cleanupConnection()

	// Wait briefly for old goroutines to finish before starting new ones.
	// Use a timeout to avoid blocking indefinitely.
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Goroutines finished
	case <-time.After(2 * time.Second):
		// Timeout - proceed anyway but log
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Timeout waiting for old goroutines")
		}
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
		fmt.Printf("%s Failed Dial: %v\n", time.Now().Format(tstamp), err)
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

	// Create a fresh context for this connection BEFORE any auth/subscription writes.
	// Also assign connection with mutex protection and clear shutdown flag.
	b.connMux.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx, b.cancel = ctx, cancel
	b.conn = conn
	b.connID++
	myConnID := b.connID
	if b.handlerQueueSz > 0 {
		b.handlerCh = make(chan string, b.handlerQueueSz)
	} else {
		b.handlerCh = nil
	}
	b.isShuttingDown = false // Clear shutdown flag on successful connection
	b.connMux.Unlock()

	// Set connected status BEFORE auth/send operations to avoid race condition
	b.setConnected(true)

	if b.requiresAuthentication() {
		if err = b.sendAuth(); err != nil {
			fmt.Println(time.Now().Format(tstamp), "Failed Connection:", fmt.Sprintf("%v", err))

			// Close connection and cancel context on authentication failure
			b.setConnected(false)
			b.connMux.Lock()
			if b.cancel != nil {
				b.cancel()
				b.cancel = nil
			}
			b.ctx = nil
			b.handlerCh = nil
			if b.conn != nil {
				b.conn.Close()
				b.conn = nil
			}
			b.connMux.Unlock()

			return nil
		}
	}

	// Start goroutines with wg.Add BEFORE launching to avoid WaitGroup misuse.
	// Start handler worker if queueing is enabled.
	b.connMux.RLock()
	handlerCh := b.handlerCh
	b.connMux.RUnlock()
	if handlerCh != nil {
		b.wg.Add(1)
		go func(ch chan string) {
			defer b.wg.Done()
			for {
				select {
				case msg := <-ch:
					if b.onMessage == nil {
						continue
					}
					handlerErr := func() (err error) {
						defer func() {
							if r := recover(); r != nil {
								err = fmt.Errorf("panic in message handler: %v", r)
							}
						}()
						return b.onMessage(msg)
					}()
					if handlerErr != nil {
						fmt.Println(time.Now().Format(tstamp), "Error handling message:", handlerErr)
					}
				case <-ctx.Done():
					return
				}
			}
		}(handlerCh)
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.handleIncomingMessages(ctx, conn, myConnID)
	}()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.monitorConnection(ctx, myConnID)
	}()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.ping(ctx, conn, myConnID)
	}()

	// Read subtopic under lock to avoid race with SendSubscription
	b.connMux.RLock()
	subtopicCopy := b.subtopic
	b.connMux.RUnlock()

	if len(subtopicCopy) > 0 {
		_, err := b.SendSubscription(subtopicCopy)
		if err != nil {
			// Cleanup resources if subscription fails
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Subscription failed, cleaning up resources:", err)
			}

			// Cancel context to stop goroutines and close connection (with mutex protection)
			b.connMux.Lock()
			if b.cancel != nil {
				b.cancel()
				b.cancel = nil
			}
			b.ctx = nil
			b.handlerCh = nil
			if b.conn != nil {
				b.conn.Close()
				b.conn = nil
			}
			b.connMux.Unlock()

			b.setConnected(false)
			return nil
		}
	}

	return b
}

func (b *WebSocket) SendSubscription(args []string) (*WebSocket, error) {
	// Copy args to avoid aliasing/user mutation races.
	argsCopy := append([]string(nil), args...)

	// Protect subtopic with connMux since it's accessed during reconnection
	b.connMux.Lock()
	b.subtopic = argsCopy
	b.connMux.Unlock()

	reqID := uuid.New().String()
	subMessage := map[string]any{
		"req_id": reqID,
		"op":     "subscribe",
		"args":   argsCopy,
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
func (b *WebSocket) sendRequest(op string, args map[string]any, headers map[string]string) error {
	reqID := uuid.New().String()
	request := map[string]any{
		"reqId":  reqID,
		"header": headers,
		"op":     op,
		"args":   []any{args},
	}
	if b.debug {
		fmt.Println("request headers:", fmt.Sprintf("%v", request["header"]))
		fmt.Println("request op channel:", fmt.Sprintf("%v", request["op"]))
		fmt.Println("request msg:", fmt.Sprintf("%v", request["args"]))
	}
	return b.sendAsJson(request)
}

func (b *WebSocket) ping(ctx context.Context, conn *websocket.Conn, id uint64) {
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
			// If a newer connection exists, stop pinging this one.
			if !b.isConnCurrent(id) {
				return
			}
			if b.isConnectedSafe() {
				// Optimized ping message creation - avoid map allocation and JSON marshaling
				currentTime := time.Now().Unix()
				pingMessage := fmt.Sprintf(`{"op":"ping","req_id":"%d"}`, currentTime)

				if err := b.sendOnConn(ctx, conn, id, pingMessage); err != nil {
					// Use optimized error checking
					if isConnectionError(err) {
						if b.debug {
							fmt.Println(time.Now().Format(tstamp), "Ping detected broken connection, triggering reconnection")
						}
						if b.isConnCurrent(id) {
							b.setConnected(false)
							b.triggerReconnect()
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

		case <-ctx.Done():
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Ping context closed, stopping ping ", b.subtopic)
			}
			return
		}
	}
}

// sendOnConn writes a message on a specific connection tied to a specific ctx/connID.
// This prevents old goroutines from writing to a newer connection after reconnect.
func (b *WebSocket) sendOnConn(ctx context.Context, conn *websocket.Conn, id uint64, message string) error {
	b.connMux.RLock()
	shuttingDown := b.isShuttingDown
	connected := b.isConnected
	current := b.connID == id && b.conn == conn
	b.connMux.RUnlock()

	if shuttingDown {
		return fmt.Errorf("connection shutting down")
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("context closed")
	}
	if !connected {
		return fmt.Errorf("websocket not connected")
	}
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	if !current {
		return fmt.Errorf("stale connection")
	}

	b.sendMux.Lock()
	defer b.sendMux.Unlock()

	// Re-check after acquiring send lock to avoid writing to a swapped-out connection.
	b.connMux.RLock()
	shuttingDown = b.isShuttingDown
	connected = b.isConnected
	current = b.connID == id && b.conn == conn
	b.connMux.RUnlock()
	if shuttingDown {
		return fmt.Errorf("connection shutting down")
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("context closed")
	}
	if !connected {
		return fmt.Errorf("websocket not connected")
	}
	if !current {
		return fmt.Errorf("stale connection")
	}

	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout * time.Second)); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "SetWriteDeadline error:", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
		return err
	}
	return nil
}

func (b *WebSocket) Disconnect() error {
	if b != nil {
		// Acquire connMux once to safely access context and connection
		b.connMux.Lock()
		var err error
		// Set shutdown flag to prevent new operations
		b.isShuttingDown = true
		if b.cancel != nil {
			b.cancel()
			b.cancel = nil // Clear for consistency with cleanupConnection
		}
		b.ctx = nil
		b.handlerCh = nil
		b.isConnected = false
		if b.conn != nil {
			b.connID++ // Invalidate in-flight goroutines tied to the old connection
			err = b.conn.Close()
			b.conn = nil
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
			// Force close connection if still open
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

	authMessage := map[string]any{
		"req_id": uuid.New().String(),
		"op":     "auth",
		"args":   []any{b.apiKey, expires, signature},
	}
	if b.debug {
		fmt.Println("auth args:", fmt.Sprintf("%v", authMessage["args"]))
	}
	return b.sendAsJson(authMessage)
}

func (b *WebSocket) sendAsJson(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.send(string(data))
}

func (b *WebSocket) send(message string) error {
	// Check connection state before acquiring send lock (follows lock hierarchy)
	b.connMux.RLock()
	conn := b.conn
	ctx := b.ctx
	connected := b.isConnected
	shuttingDown := b.isShuttingDown
	b.connMux.RUnlock()

	// Validate state without holding locks
	if shuttingDown {
		return fmt.Errorf("connection shutting down")
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("context closed")
	}
	if !connected {
		return fmt.Errorf("websocket not connected")
	}
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Now acquire send lock for actual I/O operation
	b.sendMux.Lock()

	// Re-check state after acquiring send lock to avoid writing to a stale connection
	b.connMux.RLock()
	currentConn := b.conn
	currentCtx := b.ctx
	currentConnected := b.isConnected
	currentShuttingDown := b.isShuttingDown
	b.connMux.RUnlock()
	if currentShuttingDown {
		b.sendMux.Unlock()
		return fmt.Errorf("connection shutting down")
	}
	if currentCtx != nil && currentCtx.Err() != nil {
		b.sendMux.Unlock()
		return fmt.Errorf("context closed")
	}
	if !currentConnected {
		b.sendMux.Unlock()
		return fmt.Errorf("websocket not connected")
	}
	if currentConn == nil {
		b.sendMux.Unlock()
		return fmt.Errorf("connection is nil")
	}
	if currentConn != conn || currentCtx != ctx {
		b.sendMux.Unlock()
		return fmt.Errorf("stale connection")
	}

	// Set write deadline for persistent connection reliability
	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout * time.Second)); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "SetWriteDeadline error:", err)
	}

	writeErr := conn.WriteMessage(websocket.TextMessage, []byte(message))
	b.sendMux.Unlock()
	if writeErr != nil {
		// Use optimized error checking for network errors.
		// Do NOT acquire connMux while holding sendMux (lock hierarchy).
		if isConnectionError(writeErr) {
			b.setConnected(false)
			b.triggerReconnect()
		}
		return writeErr
	}
	return nil
}
