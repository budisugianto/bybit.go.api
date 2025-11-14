package bybit_connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

// IPPoolManager manages multiple local IP addresses for outgoing WebSocket connections
// Uses least connection algorithm to distribute connections across available IPs
type IPPoolManager struct {
	ipCounters map[string]*int32 // Atomic connection counters per IP
	localAddrs []string          // List of available local IP addresses
	mu         sync.RWMutex      // Protects localAddrs slice
}

// Global IP pool shared across all WebSocket instances
var globalIPPool = &IPPoolManager{
	ipCounters: make(map[string]*int32),
	localAddrs: []string{},
}

// RegisterLocalAddresses replaces all registered local IP addresses for outgoing connections
// This is a package-level function that overwrites any previously registered IPs
func RegisterLocalAddresses(addrs []string) error {
	if len(addrs) == 0 {
		return fmt.Errorf("at least one IP address must be provided")
	}

	// Validate IP addresses
	for _, addr := range addrs {
		if net.ParseIP(addr) == nil {
			return fmt.Errorf("invalid IP address: %s", addr)
		}
	}

	globalIPPool.mu.Lock()
	defer globalIPPool.mu.Unlock()

	// Replace all IPs and reset counters
	globalIPPool.localAddrs = make([]string, len(addrs))
	copy(globalIPPool.localAddrs, addrs)
	globalIPPool.ipCounters = make(map[string]*int32)

	// Initialize atomic counters for each IP
	for _, addr := range addrs {
		var counter int32
		globalIPPool.ipCounters[addr] = &counter
	}

	return nil
}

// SelectLeastLoadedIP selects the IP with the least connections
// If multiple IPs have the same count, randomly selects among them
func (m *IPPoolManager) SelectLeastLoadedIP() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.localAddrs) == 0 {
		return "" // No IPs registered
	}

	if len(m.localAddrs) == 1 {
		return m.localAddrs[0] // Only one IP available
	}

	// Find minimum connection count
	minCount := int32(^uint32(0) >> 1) // Max int32
	for _, addr := range m.localAddrs {
		if counter, exists := m.ipCounters[addr]; exists {
			count := atomic.LoadInt32(counter)
			if count < minCount {
				minCount = count
			}
		}
	}

	// Collect all IPs with minimum count
	var candidates []string
	for _, addr := range m.localAddrs {
		if counter, exists := m.ipCounters[addr]; exists {
			if atomic.LoadInt32(counter) == minCount {
				candidates = append(candidates, addr)
			}
		}
	}

	// Random selection among candidates
	if len(candidates) == 0 {
		return m.localAddrs[0] // Fallback
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	return candidates[rand.Intn(len(candidates))]
}

// IncrementConnection atomically increments the connection count for an IP
func (m *IPPoolManager) IncrementConnection(ip string) {
	m.mu.RLock()
	counter, exists := m.ipCounters[ip]
	m.mu.RUnlock()

	if exists && counter != nil {
		atomic.AddInt32(counter, 1)
	}
}

// DecrementConnection atomically decrements the connection count for an IP
func (m *IPPoolManager) DecrementConnection(ip string) {
	m.mu.RLock()
	counter, exists := m.ipCounters[ip]
	m.mu.RUnlock()

	if exists && counter != nil {
		// Ensure count doesn't go below zero
		for {
			old := atomic.LoadInt32(counter)
			if old <= 0 {
				return
			}
			if atomic.CompareAndSwapInt32(counter, old, old-1) {
				return
			}
		}
	}
}

// GetConnectionCount returns the current connection count for an IP (for debugging)
func (m *IPPoolManager) GetConnectionCount(ip string) int32 {
	m.mu.RLock()
	counter, exists := m.ipCounters[ip]
	m.mu.RUnlock()

	if exists && counter != nil {
		return atomic.LoadInt32(counter)
	}
	return 0
}

// isConnectionError checks if an error indicates a broken connection
// Uses pre-compiled checks to avoid repeated string operations
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Check common connection errors
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection is nil") ||
		strings.Contains(strings.ToLower(msg), "not connected") ||
		strings.Contains(strings.ToLower(msg), "write: connection")
}

// triggerReconnect signals the reconnection manager to initiate reconnection
// This is non-blocking and deduplicates multiple simultaneous requests
// Safe to call from any goroutine without holding locks
func (b *WebSocket) triggerReconnect() {
	// Check shutdown state first
	b.connMux.RLock()
	shuttingDown := b.isShuttingDown
	b.connMux.RUnlock()

	if shuttingDown {
		return // Don't reconnect during shutdown
	}

	// Check if we have a valid trigger channel
	if b.reconnectTrigger == nil {
		return
	}
	select {
	case b.reconnectTrigger <- struct{}{}:
		// Successfully signaled reconnection
	default:
		// Reconnection already triggered, skip
	}
}

// reconnectionManager handles all reconnection requests through a single goroutine
func (b *WebSocket) reconnectionManager() {
	b.wg.Add(1)
	defer b.wg.Done()

	if b.debug {
		fmt.Println(time.Now().Format(tstamp), "Starting reconnection manager", b.subtopic)
	}

	for {
		select {
		case <-b.reconnectTrigger:
			// Received reconnection request
			b.ReConnect(1)
		case <-b.ctx.Done():
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Exiting reconnection manager", b.subtopic)
			}
			return
		}
	}
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

	// Capture context early to avoid race condition with nil dereference
	// This follows lock hierarchy: reconnectMux already held, now acquire connMux
	b.connMux.RLock()
	currentCtx := b.ctx
	b.connMux.RUnlock()

	// Check context validity before proceeding
	if currentCtx == nil || currentCtx.Err() != nil {
		if b.debug {
			fmt.Println(time.Now().Format(tstamp), "Context invalid, aborting reconnection ", b.subtopic)
		}
		return
	}

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
		// Re-check context before each reconnection attempt
		b.connMux.RLock()
		currentCtx = b.ctx
		b.connMux.RUnlock()

		if currentCtx == nil || currentCtx.Err() != nil {
			if b.debug {
				fmt.Println(time.Now().Format(tstamp), "Context cancelled, stopping reconnection ", b.subtopic)
			}
			return
		}

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
		case <-currentCtx.Done():
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
			// Trigger centralized reconnection
			b.triggerReconnect()
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
			// Single health check: combine connection state and timeout check
			b.connMux.RLock()
			connected := b.isConnected
			b.connMux.RUnlock()

			if !connected {
				b.triggerReconnect()
				continue
			}

			// Check receive timeout
			b.receiveMux.RLock()
			lastReceive := b.lastReceive
			b.receiveMux.RUnlock()

			// Only trigger reconnection if timeout exceeded (single trigger point)
			if time.Since(lastReceive) > time.Duration(b.pingInterval)*time.Second {
				if b.debug {
					fmt.Println(time.Now().Format(tstamp), "No data received within ping interval ", b.subtopic)
				}
				b.setConnected(false)
				b.triggerReconnect()
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
	onMessage      MessageHandler
	ctx            context.Context
	cancel         context.CancelFunc
	subtopic       []string
	isConnected    bool
	isReconnecting bool
	isShuttingDown bool   // Prevents new operations during shutdown
	boundIP        string // Sticky IP address for this connection (for reconnection)
	debug          bool
	wg             sync.WaitGroup
	// Mutex lock hierarchy (acquire in this order to prevent deadlocks):
	// 1. reconnectMux (highest level - controls reconnection state)
	// 2. connMux (protects connection state and context)
	// 3. sendMux/receiveMux (lowest level - protects I/O operations)
	reconnectMux     sync.Mutex    // Level 1: Protects reconnection state
	connMux          sync.RWMutex  // Level 2: Protects isConnected, isShuttingDown, conn, ctx, cancel, boundIP
	sendMux          sync.Mutex    // Level 3: Protects WebSocket send operations
	receiveMux       sync.RWMutex  // Level 3: Protects lastReceive and lastPong times
	reconnectTrigger chan struct{} // Centralized reconnection trigger
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

// WithLocalAddresses registers local IP addresses for this WebSocket and all future WebSockets
// This option also registers the IPs globally (same as calling RegisterLocalAddresses)
func WithLocalAddresses(addrs []string) WebsocketOption {
	return func(c *WebSocket) {
		if err := RegisterLocalAddresses(addrs); err != nil {
			fmt.Println(time.Now().Format(tstamp), "Failed to register local addresses:", err)
		}
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

	// Set finalizer to auto-decrement connection count on GC
	runtime.SetFinalizer(c, func(ws *WebSocket) {
		if ws.boundIP != "" {
			globalIPPool.DecrementConnection(ws.boundIP)
		}
	})

	return c
}

func NewBybitPublicWebSocket(url string, handler MessageHandler) *WebSocket {
	c := &WebSocket{
		url:          url,
		pingInterval: DefaultPingInterval,
		onMessage:    handler,
	}

	// Set finalizer to auto-decrement connection count on GC
	runtime.SetFinalizer(c, func(ws *WebSocket) {
		if ws.boundIP != "" {
			globalIPPool.DecrementConnection(ws.boundIP)
		}
	})

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

	// Select or reuse bound IP for sticky reconnection
	b.connMux.Lock()
	if b.boundIP == "" {
		// First connection: select IP with least connections
		b.boundIP = globalIPPool.SelectLeastLoadedIP()
		if b.boundIP != "" {
			// Increment connection count for selected IP
			globalIPPool.IncrementConnection(b.boundIP)
		}
	}
	selectedIP := b.boundIP
	b.connMux.Unlock()

	// Create dialer - use custom net dialer with local address binding if IP is selected
	var dialer *websocket.Dialer
	if selectedIP != "" {
		// Local IP registered: use custom dialer with bound local address
		netDialer := &net.Dialer{
			LocalAddr: &net.TCPAddr{
				IP: net.ParseIP(selectedIP),
			},
			Timeout: DefaultHandshakeTimeout * time.Second,
		}
		dialer = &websocket.Dialer{
			NetDial:           netDialer.Dial,
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  DefaultHandshakeTimeout * time.Second,
			ReadBufferSize:    DefaultReadBufferSize,
			WriteBufferSize:   DefaultWriteBufferSize,
			EnableCompression: true,
		}
	} else {
		// No local IP registered: fallback to default behavior (old method)
		dialer = &websocket.Dialer{
			Proxy:             http.ProxyFromEnvironment,
			HandshakeTimeout:  DefaultHandshakeTimeout * time.Second,
			ReadBufferSize:    DefaultReadBufferSize,
			WriteBufferSize:   DefaultWriteBufferSize,
			EnableCompression: true,
		}
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

	// Safely assign connection with mutex protection and clear shutdown flag
	b.connMux.Lock()
	b.conn = conn
	b.isShuttingDown = false // Clear shutdown flag on successful connection
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

	// Properly cleanup old context and wait for goroutines to finish
	if b.cancel != nil {
		b.cancel()
		// Wait briefly for old goroutines to finish before starting new ones
		// Use a timeout to avoid blocking indefinitely
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
	}

	// Create context before starting goroutines
	b.connMux.Lock()
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.connMux.Unlock()

	// Initialize reconnection trigger channel if not already created
	if b.reconnectTrigger == nil {
		b.reconnectTrigger = make(chan struct{}, 1)
		go b.reconnectionManager()
	}

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
					// Use optimized error checking
					if isConnectionError(err) {
						if b.debug {
							fmt.Println(time.Now().Format(tstamp), "Ping detected broken connection, triggering reconnection")
						}
						b.setConnected(false)
						b.triggerReconnect()
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
		// Acquire connMux once to safely access context and connection
		b.connMux.Lock()
		var err error
		// Set shutdown flag to prevent new operations
		b.isShuttingDown = true
		if b.cancel != nil {
			b.cancel()
		}
		b.isConnected = false
		if b.conn != nil {
			err = b.conn.Close()
		}
		// Decrement connection count for bound IP
		if b.boundIP != "" {
			globalIPPool.DecrementConnection(b.boundIP)
			// Note: boundIP is intentionally NOT cleared to maintain sticky behavior
			// The finalizer will also decrement, but DecrementConnection is safe against double-decrement
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
	defer b.sendMux.Unlock()

	// Set write deadline for persistent connection reliability
	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout * time.Second)); err != nil && b.debug {
		fmt.Println(time.Now().Format(tstamp), "SetWriteDeadline error:", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
		// Use optimized error checking for network errors
		if isConnectionError(err) {
			b.setConnected(false)
			b.triggerReconnect()
		}
		return err
	}
	return nil
}
