package sftp

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type MockSFTPServer struct {
	listener net.Listener
	port     int
	rootDir  string
	testDir  string
	username string
	password string
	mu       sync.Mutex
	running  bool
	wg       sync.WaitGroup
}

func NewMockSFTPServer(rootDir, username, password string) (*MockSFTPServer, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	// Create test directory within root
	testDir := filepath.Join(rootDir, "test")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		l.Close()
		return nil, err
	}

	return &MockSFTPServer{
		listener: l,
		port:     l.Addr().(*net.TCPAddr).Port,
		rootDir:  rootDir,
		testDir:  testDir,
		username: username,
		password: password,
		running:  true,
	}, nil
}

func (s *MockSFTPServer) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", s.port)
}

func (s *MockSFTPServer) TestDir() string {
	return s.testDir
}

func (s *MockSFTPServer) Username() string {
	return s.username
}

func (s *MockSFTPServer) Password() string {
	return s.password
}

func (s *MockSFTPServer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				s.mu.Lock()
				if !s.running {
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
				continue
			}
			go s.handleConnection(conn)
		}
	}()
}

func (s *MockSFTPServer) Stop() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

func (s *MockSFTPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Configure SSH server
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == s.username && string(pass) == s.password {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected for %q", c.User())
		},
	}

	// Generate a temporary host key
	privateKey, err := generatePrivateKey()
	if err != nil {
		return
	}

	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		return
	}
	config.AddHostKey(signer)

	// Perform SSH handshake
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return
	}
	defer sshConn.Close()

	// Discard all global requests
	go ssh.DiscardRequests(reqs)

	// Handle channels
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}

		go func(in <-chan *ssh.Request) {
			for req := range in {
				ok := false
				if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
					ok = true
				}
				req.Reply(ok, nil)
			}
		}(requests)

		// Start SFTP server
		go func() {
			defer channel.Close()
			server, err := sftp.NewServer(
				channel,
				sftp.WithServerWorkingDirectory(s.rootDir),
			)
			if err != nil {
				return
			}
			if err := server.Serve(); err != nil && err != io.EOF {
				return
			}
		}()
	}
}

func generatePrivateKey() (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}
