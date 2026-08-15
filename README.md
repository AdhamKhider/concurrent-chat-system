# Concurrent Chat System (Terminal UI)

A single-process concurrent chat simulation written in Go. Each connected user is represented by a client goroutine, while a central server goroutine coordinates join, message, and leave events through channels and `select`.

## Requirements covered

| Assignment requirement | Implementation |
| --- | --- |
| Goroutines | The server event loop and every connected client's listener run in goroutines. |
| Channels | `joinPipe`, `messagePipe`, `leavePipe`, per-client `incoming`, and shutdown channels carry all chat events. |
| `select` | The server uses `select` to process join, message, leave, and shutdown events. Client listeners use `select` for incoming notifications and shutdown. |
| `sync.Mutex` | The server mutex protects the connected-user map; the console mutex protects terminal output. |
| Duplicate usernames | Usernames are normalized case-insensitively and duplicate joins are rejected. |
| Sender exclusion | Broadcasts are delivered to every connected user except the sender. The terminal prints a local echo for the sender. |
| Immediate display | Each client listener prints notifications as soon as its incoming channel receives them. |
| Graceful shutdown | `quit`, end-of-input, Ctrl+C, and SIGTERM stop all clients and wait for server and client goroutines. |
| Standard library only | The project has no third-party dependencies. |

## Run the application

```bash
go run .
```

The application displays a command menu:

```text
join <username>    Create and connect a new user
users              List connected users
select <username>  Switch the active user
send <message>     Send a message as the active user
remove <username>  Disconnect a user
who                Show the active user
help               Show the command list
quit / exit        Shut down the chat system
```

## Example demonstration flow

```text
join sara
send hello
join mohamed
send hiii
select mohamed
send hello sara
who
users
remove mohamed
quit
```

Messages received by a user appear with that user's display prefix. For example, when Sara sends a message while Mohamed is connected, the terminal displays both the local echo and the asynchronous notification:

```text
sara: hiii
[mohamed] sara: hiii
```

The sender does not receive a broadcast copy of their own message. When Mohamed leaves, Sara receives:

```text
[sara] User mohamed left the chat.
```

## Test the project

Run the unit tests:

```bash
go test ./...
```

Run them with the race detector:

```bash
go test -race ./...
```

The tests cover the join/message/leave lifecycle, duplicate-name rejection, sender exclusion, concurrent joins, and protected shared state.

## Suggested recording outline

For a short demonstration video, first show the project files and briefly explain that `ChatServer.run` is the central `select` loop, the three event pipes carry join/message/leave requests, and every `Client` has an independent listener goroutine. Then run the application and demonstrate `join sara`, `send hello`, `join mohamed`, `send hiii`, `select mohamed`, `send hello sara`, `who`, `users`, `remove mohamed`, and `quit`. Finish by running `go test -race ./...` and showing the successful result.
