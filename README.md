# NoLag Go SDK

Real-time messaging SDK for Go applications.

## Installation

```bash
go get github.com/NoLagApp/nolag-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    nolag "github.com/NoLagApp/nolag-go"
)

func main() {
    // Create client with your actor token
    client := nolag.New("your-actor-token")

    // Connect to NoLag
    if err := client.Connect(); err != nil {
        panic(err)
    }
    defer client.Close()

    // Subscribe to a topic
    client.Subscribe("my-topic", func(data any, meta nolag.MessageMeta) {
        fmt.Printf("Received: %v\n", data)
    })

    // Publish a message
    client.Emit("my-topic", map[string]any{"hello": "world"})

    // Keep running
    time.Sleep(60 * time.Second)
}
```

## Configuration

```go
import (
    "time"
    nolag "github.com/NoLagApp/nolag-go"
)

options := nolag.Options{
    URL:                  "wss://broker.nolag.app/ws", // Custom broker URL
    Reconnect:            true,                         // Auto-reconnect
    ReconnectInterval:    5 * time.Second,              // Reconnect interval
    MaxReconnectAttempts: 10,                           // Max attempts (0 = infinite)
    HeartbeatInterval:    30 * time.Second,             // Heartbeat interval (0 to disable)
    QoS:                  nolag.QoSAtLeastOnce,         // Default QoS
    Debug:                true,                         // Enable debug logging
}

client := nolag.New("your-actor-token", options)
```

## Subscribing to Topics

```go
// Basic subscription
client.Subscribe("chat/messages", func(data any, meta nolag.MessageMeta) {
    fmt.Printf("Message from %s: %v\n", meta.Sender, data)
})

// With options
qos := nolag.QoSExactlyOnce
loadBalance := true
client.Subscribe("tasks", handler, nolag.SubscribeOptions{
    QoS:              &qos,
    LoadBalance:      &loadBalance,
    LoadBalanceGroup: "workers",
})

// Unsubscribe
client.Unsubscribe("chat/messages")
```

## Publishing Messages

```go
// Publish any data (maps, structs, strings, etc.)
client.Emit("chat/messages", map[string]any{"text": "Hello!"})

// With options
qos := nolag.QoSExactlyOnce
client.Emit("status", map[string]any{"online": true}, nolag.EmitOptions{
    QoS:    &qos,
    Retain: true, // Retain last message for new subscribers
})
```

## Connection Events

```go
// Listen for connection events
client.On("connected", func(args ...any) {
    fmt.Println("Connected!")
})

client.On("disconnected", func(args ...any) {
    fmt.Println("Disconnected")
})

client.On("reconnecting", func(args ...any) {
    attempt := args[0].(int)
    fmt.Printf("Reconnecting... attempt %d\n", attempt)
})

client.On("error", func(args ...any) {
    err := args[0].(error)
    fmt.Printf("Error: %v\n", err)
})

// Check connection status
if client.Status() == nolag.StatusConnected {
    fmt.Println("We're connected!")
}
```

## Presence

```go
// Set your presence data
client.SetPresence(map[string]any{
    "status": "online",
    "typing": false,
})

// Get presence of all actors in a topic
presenceList, err := client.GetPresence("chat/room-1")
if err == nil {
    for _, actor := range presenceList {
        fmt.Printf("%s: %v\n", actor.ActorTokenID, actor.Presence)
    }
}

// Listen for presence changes
client.On("presence", func(args ...any) {
    topic := args[0].(string)
    presence := args[1]
    fmt.Printf("Presence update in %s: %v\n", topic, presence)
})
```

## Error Handling

```go
package main

import (
    "fmt"
    nolag "github.com/NoLagApp/nolag-go"
)

func main() {
    client := nolag.New("your-actor-token")

    // Handle errors via event
    client.On("error", func(args ...any) {
        if err, ok := args[0].(error); ok {
            fmt.Printf("Error: %v\n", err)
        }
    })

    // Connect with error handling
    if err := client.Connect(); err != nil {
        fmt.Printf("Connection failed: %v\n", err)
        return
    }
    defer client.Close()

    // Operations return errors
    if err := client.Emit("topic", "data"); err != nil {
        fmt.Printf("Emit failed: %v\n", err)
    }
}
```

## QoS Levels

| Level | Constant | Description |
|-------|----------|-------------|
| 0 | `QoSAtMostOnce` | Fire and forget, no acknowledgment |
| 1 | `QoSAtLeastOnce` | Guaranteed delivery, may have duplicates |
| 2 | `QoSExactlyOnce` | Guaranteed exactly one delivery |

```go
// Set default QoS in options
options := nolag.Options{
    QoS: nolag.QoSExactlyOnce,
}

// Or per-message
qos := nolag.QoSExactlyOnce
client.Emit("important", data, nolag.EmitOptions{QoS: &qos})
```

## Load Balancing

Distribute messages across multiple subscribers:

```go
loadBalance := true
client.Subscribe("tasks", processTask, nolag.SubscribeOptions{
    LoadBalance:      &loadBalance,
    LoadBalanceGroup: "task-workers",
})
```

## Type Definitions

```go
import nolag "github.com/NoLagApp/nolag-go"

// Main types
nolag.Client           // The client
nolag.Options          // Connection options
nolag.SubscribeOptions // Subscription options
nolag.EmitOptions      // Publish options

// Enums
nolag.ConnectionStatus // StatusDisconnected, StatusConnecting, etc.
nolag.ActorType        // ActorDevice, ActorUser, ActorServer, ActorSession
nolag.QoS              // QoSAtMostOnce, QoSAtLeastOnce, QoSExactlyOnce

// Data types
nolag.MessageMeta      // Message metadata (Sender, Timestamp)
nolag.ActorPresence    // Presence info
nolag.MessageHandler   // func(data any, meta MessageMeta)
nolag.EventHandler     // func(args ...any)
```

## Requirements

- Go 1.21+
- github.com/gorilla/websocket v1.5.1
- github.com/vmihailenco/msgpack/v5 v5.4.1

## License

MIT
