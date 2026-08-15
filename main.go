package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
)

var (
	errDuplicateUsername = errors.New("username already exists")
	errUserNotFound      = errors.New("user not found")
	errEmptyUsername     = errors.New("username cannot be empty")
)

// Console serializes all terminal writes so that messages printed by client
// goroutines do not interleave at the byte level.
type Console struct {
	mu     sync.Mutex
	writer io.Writer
}

func NewConsole(writer io.Writer) *Console {
	return &Console{writer: writer}
}

func (c *Console) Print(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, _ = io.WriteString(c.writer, text)
}

func (c *Console) Printf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, _ = fmt.Fprintf(c.writer, format, args...)
}

// Client represents one connected chat user. Its incoming channel is consumed
// by an independent goroutine, which makes incoming messages asynchronous.
type Client struct {
	name string

	incoming chan string
	done     chan struct{}

	stopOnce sync.Once
}

func NewClient(name string) *Client {
	return &Client{
		name:     name,
		incoming: make(chan string, 16),
		done:     make(chan struct{}),
	}
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) start(console *Console, clientsWG *sync.WaitGroup) {
	clientsWG.Add(1)
	go func() {
		defer clientsWG.Done()
		for {
			select {
			case notification, ok := <-c.incoming:
				if !ok {
					return
				}
				console.Printf("%s\n", notification)
			case <-c.done:
				return
			}
		}
	}()
}

func (c *Client) shutdown() {
	c.stopOnce.Do(func() {
		close(c.done)
	})
}

func normalizeUsername(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

type joinRequest struct {
	client   *Client
	response chan error
}

type messageRequest struct {
	from     string
	text     string
	response chan error
}

type leaveRequest struct {
	name     string
	response chan leaveResponse
}

type leaveResponse struct {
	name string
	err  error
}

// ChatServer owns the three required event pipes. The server goroutine is the
// single event coordinator and uses select to process joins, messages, leaves,
// and shutdown requests concurrently.
type ChatServer struct {
	joinPipe    chan joinRequest
	messagePipe chan messageRequest
	leavePipe   chan leaveRequest
	shutdown    chan chan struct{}

	console *Console

	mu    sync.Mutex
	users map[string]*Client

	serverWG  sync.WaitGroup
	clientsWG sync.WaitGroup
}

func NewChatServer(console *Console) *ChatServer {
	return &ChatServer{
		joinPipe:    make(chan joinRequest),
		messagePipe: make(chan messageRequest),
		leavePipe:   make(chan leaveRequest),
		shutdown:    make(chan chan struct{}),
		console:     console,
		users:       make(map[string]*Client),
	}
}

func (s *ChatServer) Start() {
	s.serverWG.Add(1)
	go s.run()
}

func (s *ChatServer) run() {
	defer s.serverWG.Done()

	for {
		select {
		case request := <-s.joinPipe:
			s.handleJoin(request)
		case request := <-s.messagePipe:
			s.handleMessage(request)
		case request := <-s.leavePipe:
			s.handleLeave(request)
		case complete := <-s.shutdown:
			s.handleShutdown()
			close(complete)
			return
		}
	}
}

func (s *ChatServer) handleJoin(request joinRequest) {
	client := request.client
	key := normalizeUsername(client.Name())
	if key == "" {
		request.response <- errEmptyUsername
		return
	}

	s.mu.Lock()
	if _, exists := s.users[key]; exists {
		s.mu.Unlock()
		request.response <- errDuplicateUsername
		return
	}

	recipients := make([]*Client, 0, len(s.users))
	for _, existing := range s.users {
		recipients = append(recipients, existing)
	}
	s.users[key] = client
	s.mu.Unlock()

	client.start(s.console, &s.clientsWG)
	for _, recipient := range recipients {
		s.deliver(recipient, fmt.Sprintf("[%s] User %s joined the chat.", recipient.Name(), client.Name()))
	}

	request.response <- nil
}

func (s *ChatServer) handleMessage(request messageRequest) {
	key := normalizeUsername(request.from)
	if key == "" || strings.TrimSpace(request.text) == "" {
		request.response <- errors.New("message cannot be empty")
		return
	}

	s.mu.Lock()
	sender, exists := s.users[key]
	recipients := make([]*Client, 0, len(s.users))
	if exists {
		for recipientKey, recipient := range s.users {
			if recipientKey != key {
				recipients = append(recipients, recipient)
			}
		}
	}
	s.mu.Unlock()

	if !exists {
		request.response <- errUserNotFound
		return
	}

	for _, recipient := range recipients {
		s.deliver(recipient, fmt.Sprintf("[%s] %s: %s", recipient.Name(), sender.Name(), request.text))
	}

	request.response <- nil
}

func (s *ChatServer) handleLeave(request leaveRequest) {
	key := normalizeUsername(request.name)
	if key == "" {
		request.response <- leaveResponse{err: errUserNotFound}
		return
	}

	s.mu.Lock()
	departing, exists := s.users[key]
	if !exists {
		s.mu.Unlock()
		request.response <- leaveResponse{err: errUserNotFound}
		return
	}

	delete(s.users, key)
	recipients := make([]*Client, 0, len(s.users))
	for _, recipient := range s.users {
		recipients = append(recipients, recipient)
	}
	s.mu.Unlock()

	departing.shutdown()
	for _, recipient := range recipients {
		s.deliver(recipient, fmt.Sprintf("[%s] User %s left the chat.", recipient.Name(), departing.Name()))
	}

	request.response <- leaveResponse{name: departing.Name()}
}

func (s *ChatServer) handleShutdown() {
	s.mu.Lock()
	clients := make([]*Client, 0, len(s.users))
	for _, client := range s.users {
		clients = append(clients, client)
	}
	s.users = make(map[string]*Client)
	s.mu.Unlock()

	for _, client := range clients {
		client.shutdown()
	}
	s.clientsWG.Wait()
}

func (s *ChatServer) deliver(client *Client, notification string) {
	select {
	case client.incoming <- notification:
	case <-client.done:
	}
}

func (s *ChatServer) Join(ctx context.Context, client *Client) error {
	request := joinRequest{client: client, response: make(chan error, 1)}
	select {
	case s.joinPipe <- request:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-request.response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChatServer) Send(ctx context.Context, from, text string) error {
	request := messageRequest{from: from, text: text, response: make(chan error, 1)}
	select {
	case s.messagePipe <- request:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-request.response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChatServer) Leave(ctx context.Context, name string) (string, error) {
	request := leaveRequest{name: name, response: make(chan leaveResponse, 1)}
	select {
	case s.leavePipe <- request:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	select {
	case response := <-request.response:
		return response.name, response.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *ChatServer) FindUser(name string) (string, bool) {
	key := normalizeUsername(name)
	s.mu.Lock()
	defer s.mu.Unlock()
	client, exists := s.users[key]
	if !exists {
		return "", false
	}
	return client.Name(), true
}

func (s *ChatServer) ListUsers() []string {
	s.mu.Lock()
	users := make([]string, 0, len(s.users))
	for _, client := range s.users {
		users = append(users, client.Name())
	}
	s.mu.Unlock()

	sort.Slice(users, func(i, j int) bool {
		return strings.ToLower(users[i]) < strings.ToLower(users[j])
	})
	return users
}

func (s *ChatServer) Shutdown(ctx context.Context) error {
	complete := make(chan struct{})
	select {
	case s.shutdown <- complete:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-complete:
		s.serverWG.Wait()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func printHelp(console *Console) {
	console.Print("\nCommands:\n")
	console.Print("  join <username>   Create and connect a new user\n")
	console.Print("  users             List connected users\n")
	console.Print("  select <username> Switch the active user\n")
	console.Print("  send <message>    Send a message as the active user\n")
	console.Print("  remove <username> Disconnect a user\n")
	console.Print("  who               Show the active user\n")
	console.Print("  help              Show this command list\n")
	console.Print("  quit / exit       Shut down the chat system\n\n")
}

func printPrompt(console *Console, activeUser string) {
	if activeUser == "" {
		console.Print("> ")
		return
	}
	console.Printf("(now acting as %s) > ", activeUser)
}

func main() {
	console := NewConsole(os.Stdout)
	server := NewChatServer(console)
	server.Start()

	ctx := context.Background()
	console.Print("Concurrent Chat System (Terminal UI)\n")
	printHelp(console)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	normalExit := make(chan struct{})
	go func() {
		select {
		case <-signalChannel:
			console.Print("\nShutting down...\n")
			_ = server.Shutdown(context.Background())
			os.Exit(0)
		case <-normalExit:
		}
	}()
	defer signal.Stop(signalChannel)

	activeUser := ""
	scanner := bufio.NewScanner(os.Stdin)
	for {
		printPrompt(console, activeUser)
		if !scanner.Scan() {
			console.Print("\nInput closed. Shutting down...\n")
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		command := strings.ToLower(fields[0])

		switch command {
		case "join":
			if len(fields) != 2 {
				console.Print("Usage: join <username>\n")
				continue
			}
			client := NewClient(fields[1])
			if err := server.Join(ctx, client); err != nil {
				console.Printf("Could not join %s: %s.\n", fields[1], err)
				continue
			}
			actualName, _ := server.FindUser(fields[1])
			console.Printf("User %s joined the chat.\n", actualName)
			if activeUser == "" {
				activeUser = actualName
			}

		case "users":
			users := server.ListUsers()
			console.Print("Connected users:\n")
			if len(users) == 0 {
				console.Print("  (none)\n")
				continue
			}
			for _, user := range users {
				console.Printf("  - %s\n", user)
			}

		case "select":
			if len(fields) != 2 {
				console.Print("Usage: select <username>\n")
				continue
			}
			actualName, exists := server.FindUser(fields[1])
			if !exists {
				console.Printf("Cannot select %s: user is not connected.\n", fields[1])
				continue
			}
			activeUser = actualName
			console.Printf("Now acting as %s.\n", activeUser)

		case "send":
			if activeUser == "" {
				console.Print("Select or join a user before sending a message.\n")
				continue
			}
			message := strings.TrimSpace(strings.TrimPrefix(line, fields[0]))
			if message == "" {
				console.Print("Usage: send <message>\n")
				continue
			}
			actualName, exists := server.FindUser(activeUser)
			if !exists {
				activeUser = ""
				console.Print("The active user is no longer connected.\n")
				continue
			}
			if err := server.Send(ctx, actualName, message); err != nil {
				console.Printf("Could not send message: %s.\n", err)
				continue
			}
			// This is a local echo for the selected user, not a server delivery.
			// The sender still does not receive its own broadcast notification.
			console.Printf("%s: %s\n", actualName, message)

		case "remove":
			if len(fields) != 2 {
				console.Print("Usage: remove <username>\n")
				continue
			}
			removedName, err := server.Leave(ctx, fields[1])
			if err != nil {
				console.Printf("Could not remove %s: user is not connected.\n", fields[1])
				continue
			}
			console.Printf("User %s left the chat.\n", removedName)
			if strings.EqualFold(activeUser, removedName) {
				activeUser = ""
			}

		case "who":
			if activeUser == "" {
				console.Print("Currently acting as: none\n")
			} else {
				console.Printf("Currently acting as: %s\n", activeUser)
			}

		case "help":
			printHelp(console)

		case "quit", "exit":
			console.Print("Shutting down...\n")
			close(normalExit)
			_ = server.Shutdown(ctx)
			return

		default:
			console.Printf("Unknown command %q. Type help for available commands.\n", fields[0])
		}
	}

	close(normalExit)
	_ = server.Shutdown(ctx)
}
