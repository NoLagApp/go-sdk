package nolag

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/pion/webrtc/v3"
)

// WebRTC errors
var (
	ErrWebRTCNotStarted   = errors.New("WebRTC manager not started")
	ErrWebRTCAlreadyStarted = errors.New("WebRTC manager already started")
	ErrClientNotConnected = errors.New("NoLag client not connected")
	ErrNoActorID          = errors.New("actor ID not available")
)

// Default STUN servers
var DefaultICEServers = []webrtc.ICEServer{
	{URLs: []string{"stun:stun.l.google.com:19302"}},
	{URLs: []string{"stun:stun1.l.google.com:19302"}},
}

// WebRTCOptions configures the WebRTC manager
type WebRTCOptions struct {
	App        string
	Room       string
	ICEServers []webrtc.ICEServer
}

// PeerState holds state for a peer connection
type PeerState struct {
	ActorID      string
	PC           *webrtc.PeerConnection
	Polite       bool
	MakingOffer  bool
	IgnoreOffer  bool
	RemoteTracks []*webrtc.TrackRemote
	mu           sync.Mutex
}

// WebRTCEvent types
type WebRTCEvent string

const (
	EventPeerConnected    WebRTCEvent = "peer_connected"
	EventPeerDisconnected WebRTCEvent = "peer_disconnected"
	EventPeerTrack        WebRTCEvent = "peer_track"
	EventLocalTrack       WebRTCEvent = "local_track"
	EventError            WebRTCEvent = "error"
)

// WebRTCEventHandler handles WebRTC events
type WebRTCEventHandler func(args ...any)

// TrackHandler handles incoming tracks from peers
type TrackHandler func(actorID string, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver)

// WebRTCManager manages peer-to-peer WebRTC connections using NoLag for signaling.
// Uses pion/webrtc for WebRTC implementation in Go.
//
// Example:
//
//	webrtc := nolag.NewWebRTCManager(client, nolag.WebRTCOptions{
//	    App:  "video-chat",
//	    Room: "meeting-123",
//	})
//
//	webrtc.OnTrack(func(actorID string, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
//	    // Process incoming audio/video
//	})
//
//	webrtc.Start()
type WebRTCManager struct {
	client  *Client
	options WebRTCOptions
	room    *Room

	localTracks   []webrtc.TrackLocal
	peers         map[string]*PeerState
	started       bool
	eventHandlers map[WebRTCEvent][]WebRTCEventHandler
	trackHandler  TrackHandler

	mu sync.RWMutex
}

// NewWebRTCManager creates a new WebRTC manager
func NewWebRTCManager(client *Client, options WebRTCOptions) *WebRTCManager {
	if len(options.ICEServers) == 0 {
		options.ICEServers = DefaultICEServers
	}

	return &WebRTCManager{
		client:        client,
		options:       options,
		room:          client.SetApp(options.App).SetRoom(options.Room),
		peers:         make(map[string]*PeerState),
		eventHandlers: make(map[WebRTCEvent][]WebRTCEventHandler),
	}
}

// TopicPrefix returns the full topic prefix (app/room)
func (m *WebRTCManager) TopicPrefix() string {
	return fmt.Sprintf("%s/%s", m.options.App, m.options.Room)
}

// MyActorID returns the local actor ID
func (m *WebRTCManager) MyActorID() string {
	return m.client.ActorID()
}

// IsStarted returns whether the manager is started
func (m *WebRTCManager) IsStarted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}

// AddTrack adds a local track to share with peers
func (m *WebRTCManager) AddTrack(track webrtc.TrackLocal) error {
	m.mu.Lock()
	m.localTracks = append(m.localTracks, track)
	m.mu.Unlock()

	m.emit(EventLocalTrack, track)

	// Add to existing peer connections
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, peer := range m.peers {
		_, err := peer.PC.AddTrack(track)
		if err != nil {
			return fmt.Errorf("failed to add track to peer %s: %w", peer.ActorID, err)
		}
	}

	return nil
}

// GetPeers returns list of connected peer IDs
func (m *WebRTCManager) GetPeers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]string, 0, len(m.peers))
	for id := range m.peers {
		peers = append(peers, id)
	}
	return peers
}

// IsConnected checks if connected to a specific peer
func (m *WebRTCManager) IsConnected(actorID string) bool {
	m.mu.RLock()
	peer, ok := m.peers[actorID]
	m.mu.RUnlock()

	if !ok {
		return false
	}

	return peer.PC.ConnectionState() == webrtc.PeerConnectionStateConnected
}

// Start initializes the WebRTC manager
// - Subscribes to signaling topics
// - Listens for presence events
// - Initiates connections to existing peers
func (m *WebRTCManager) Start() error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return ErrWebRTCAlreadyStarted
	}

	if m.client.Status() != StatusConnected {
		m.mu.Unlock()
		return ErrClientNotConnected
	}

	m.started = true
	m.mu.Unlock()

	// Subscribe to signaling topics
	topics := []string{"webrtc:offer", "webrtc:answer", "webrtc:candidate"}
	for _, topic := range topics {
		handler := m.createSignalingHandler(topic)
		if err := m.room.Subscribe(topic, handler); err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", topic, err)
		}
	}

	// Listen to presence events
	m.client.On("presence", m.handlePresence)

	// Set presence with webrtcReady flag
	if err := m.client.SetPresence(map[string]any{"webrtcReady": true}); err != nil {
		return fmt.Errorf("failed to set presence: %w", err)
	}

	return nil
}

// Stop cleans up the WebRTC manager
// - Closes all peer connections
// - Unsubscribes from signaling topics
// - Removes event listeners
func (m *WebRTCManager) Stop() error {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = false
	m.mu.Unlock()

	// Close all peer connections
	m.mu.Lock()
	for actorID, peer := range m.peers {
		peer.PC.Close()
		delete(m.peers, actorID)
		m.emit(EventPeerDisconnected, actorID)
	}
	m.mu.Unlock()

	// Unsubscribe from signaling topics
	topics := []string{"webrtc:offer", "webrtc:answer", "webrtc:candidate"}
	for _, topic := range topics {
		m.room.Unsubscribe(topic)
	}

	// Remove handlers
	m.client.Off("presence")

	// Clear presence
	m.client.SetPresence(map[string]any{"webrtcReady": false})

	return nil
}

// On registers an event handler
func (m *WebRTCManager) On(event WebRTCEvent, handler WebRTCEventHandler) *WebRTCManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventHandlers[event] = append(m.eventHandlers[event], handler)
	return m
}

// Off removes all handlers for an event
func (m *WebRTCManager) Off(event WebRTCEvent) *WebRTCManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.eventHandlers, event)
	return m
}

// OnTrack sets the handler for incoming tracks
func (m *WebRTCManager) OnTrack(handler TrackHandler) *WebRTCManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trackHandler = handler
	return m
}

func (m *WebRTCManager) emit(event WebRTCEvent, args ...any) {
	m.mu.RLock()
	handlers := m.eventHandlers[event]
	m.mu.RUnlock()

	for _, h := range handlers {
		go h(args...)
	}
}

func (m *WebRTCManager) handlePresence(args ...any) {
	if len(args) < 2 {
		return
	}

	// Parse presence data
	data, ok := args[1].(map[string]any)
	if !ok {
		return
	}

	actorID, _ := data["actor_token_id"].(string)
	presence, _ := data["presence"].(map[string]any)
	event, _ := data["event"].(string)

	if actorID == "" || actorID == m.MyActorID() {
		return
	}

	if event == "join" && presence["webrtcReady"] == true {
		m.mu.RLock()
		_, exists := m.peers[actorID]
		m.mu.RUnlock()

		if !exists {
			go m.createPeerConnection(actorID)
		}
	} else if event == "leave" {
		m.mu.Lock()
		if peer, ok := m.peers[actorID]; ok {
			peer.PC.Close()
			delete(m.peers, actorID)
		}
		m.mu.Unlock()
		m.emit(EventPeerDisconnected, actorID)
	}
}

func (m *WebRTCManager) createPeerConnection(remoteActorID string) (*PeerState, error) {
	config := webrtc.Configuration{
		ICEServers: m.options.ICEServers,
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer connection: %w", err)
	}

	// Determine politeness for perfect negotiation
	polite := m.MyActorID() < remoteActorID

	peerState := &PeerState{
		ActorID: remoteActorID,
		PC:      pc,
		Polite:  polite,
	}

	m.mu.Lock()
	m.peers[remoteActorID] = peerState
	m.mu.Unlock()

	// Add local tracks
	m.mu.RLock()
	for _, track := range m.localTracks {
		if _, err := pc.AddTrack(track); err != nil {
			m.mu.RUnlock()
			return nil, fmt.Errorf("failed to add track: %w", err)
		}
	}
	m.mu.RUnlock()

	// Handle ICE candidates
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		m.sendCandidate(remoteActorID, candidate.ToJSON())
	})

	// Handle remote tracks
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		peerState.mu.Lock()
		peerState.RemoteTracks = append(peerState.RemoteTracks, track)
		peerState.mu.Unlock()

		m.emit(EventPeerTrack, remoteActorID, track)
		m.emit(EventPeerConnected, remoteActorID, track)

		m.mu.RLock()
		handler := m.trackHandler
		m.mu.RUnlock()

		if handler != nil {
			handler(remoteActorID, track, receiver)
		}
	})

	// Handle connection state changes
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateDisconnected ||
			state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed {
			m.mu.Lock()
			delete(m.peers, remoteActorID)
			m.mu.Unlock()
			m.emit(EventPeerDisconnected, remoteActorID)
		}
	})

	// Handle negotiation needed
	pc.OnNegotiationNeeded(func() {
		peerState.mu.Lock()
		peerState.MakingOffer = true
		peerState.mu.Unlock()

		offer, err := pc.CreateOffer(nil)
		if err != nil {
			m.emit(EventError, err)
			peerState.mu.Lock()
			peerState.MakingOffer = false
			peerState.mu.Unlock()
			return
		}

		if err := pc.SetLocalDescription(offer); err != nil {
			m.emit(EventError, err)
			peerState.mu.Lock()
			peerState.MakingOffer = false
			peerState.mu.Unlock()
			return
		}

		m.sendOffer(remoteActorID, pc.LocalDescription())

		peerState.mu.Lock()
		peerState.MakingOffer = false
		peerState.mu.Unlock()
	})

	return peerState, nil
}

func (m *WebRTCManager) createSignalingHandler(topic string) MessageHandler {
	return func(data any, meta MessageMeta) {
		dataMap, ok := data.(map[string]any)
		if !ok {
			return
		}

		switch topic {
		case "webrtc:offer":
			m.handleOffer(dataMap)
		case "webrtc:answer":
			m.handleAnswer(dataMap)
		case "webrtc:candidate":
			m.handleCandidate(dataMap)
		}
	}
}

func (m *WebRTCManager) handleOffer(data map[string]any) {
	targetActorID, _ := data["targetActorId"].(string)
	if targetActorID != m.MyActorID() {
		return
	}

	senderActorID, _ := data["senderActorId"].(string)
	if senderActorID == "" {
		return
	}

	m.mu.RLock()
	peer, ok := m.peers[senderActorID]
	m.mu.RUnlock()

	if !ok {
		var err error
		peer, err = m.createPeerConnection(senderActorID)
		if err != nil {
			m.emit(EventError, err)
			return
		}
	}

	// Perfect negotiation: handle offer collision
	peer.mu.Lock()
	offerCollision := peer.MakingOffer || peer.PC.SignalingState() != webrtc.SignalingStateStable
	peer.IgnoreOffer = !peer.Polite && offerCollision
	peer.mu.Unlock()

	if peer.IgnoreOffer {
		return
	}

	sdp, _ := data["sdp"].(string)
	sessionID, _ := data["sessionId"].(string)

	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}

	if err := peer.PC.SetRemoteDescription(offer); err != nil {
		m.emit(EventError, err)
		return
	}

	answer, err := peer.PC.CreateAnswer(nil)
	if err != nil {
		m.emit(EventError, err)
		return
	}

	if err := peer.PC.SetLocalDescription(answer); err != nil {
		m.emit(EventError, err)
		return
	}

	m.sendAnswer(senderActorID, peer.PC.LocalDescription(), sessionID)
}

func (m *WebRTCManager) handleAnswer(data map[string]any) {
	targetActorID, _ := data["targetActorId"].(string)
	if targetActorID != m.MyActorID() {
		return
	}

	senderActorID, _ := data["senderActorId"].(string)
	if senderActorID == "" {
		return
	}

	m.mu.RLock()
	peer, ok := m.peers[senderActorID]
	m.mu.RUnlock()

	if !ok {
		return
	}

	sdp, _ := data["sdp"].(string)
	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}

	if err := peer.PC.SetRemoteDescription(answer); err != nil {
		m.emit(EventError, err)
	}
}

func (m *WebRTCManager) handleCandidate(data map[string]any) {
	targetActorID, _ := data["targetActorId"].(string)
	if targetActorID != m.MyActorID() {
		return
	}

	senderActorID, _ := data["senderActorId"].(string)
	if senderActorID == "" {
		return
	}

	m.mu.RLock()
	peer, ok := m.peers[senderActorID]
	m.mu.RUnlock()

	if !ok || peer.IgnoreOffer {
		return
	}

	candidateData, ok := data["candidate"].(map[string]any)
	if !ok {
		return
	}

	candidateJSON, err := json.Marshal(candidateData)
	if err != nil {
		return
	}

	var candidate webrtc.ICECandidateInit
	if err := json.Unmarshal(candidateJSON, &candidate); err != nil {
		return
	}

	if err := peer.PC.AddICECandidate(candidate); err != nil {
		if !peer.IgnoreOffer {
			m.emit(EventError, err)
		}
	}
}

func (m *WebRTCManager) sendOffer(targetActorID string, description *webrtc.SessionDescription) {
	sessionID := fmt.Sprintf("sess_%d", m.generateID())
	message := map[string]any{
		"type":          "offer",
		"senderActorId": m.MyActorID(),
		"targetActorId": targetActorID,
		"sessionId":     sessionID,
		"sdp":           description.SDP,
	}
	m.room.Emit("webrtc:offer", message, EmitOptions{Echo: boolPtr(false)})
}

func (m *WebRTCManager) sendAnswer(targetActorID string, description *webrtc.SessionDescription, sessionID string) {
	message := map[string]any{
		"type":          "answer",
		"senderActorId": m.MyActorID(),
		"targetActorId": targetActorID,
		"sessionId":     sessionID,
		"sdp":           description.SDP,
	}
	m.room.Emit("webrtc:answer", message, EmitOptions{Echo: boolPtr(false)})
}

func (m *WebRTCManager) sendCandidate(targetActorID string, candidate webrtc.ICECandidateInit) {
	message := map[string]any{
		"senderActorId": m.MyActorID(),
		"targetActorId": targetActorID,
		"candidate":     candidate,
	}
	m.room.Emit("webrtc:candidate", message, EmitOptions{Echo: boolPtr(false)})
}

var idCounter int64
var idMu sync.Mutex

func (m *WebRTCManager) generateID() int64 {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return idCounter
}

func boolPtr(b bool) *bool {
	return &b
}
