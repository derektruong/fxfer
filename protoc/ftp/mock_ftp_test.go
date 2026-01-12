package ftp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type MockFTPServer struct {
	listener net.Listener
	port     int
	rootDir  string
	mu       sync.Mutex
	running  bool
	wg       sync.WaitGroup
}

func NewMockFTPServer(rootDir string) (*MockFTPServer, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &MockFTPServer{
		listener: l,
		port:     l.Addr().(*net.TCPAddr).Port,
		rootDir:  rootDir,
		running:  true,
	}, nil
}

func (s *MockFTPServer) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", s.port)
}

func (s *MockFTPServer) Start() {
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

func (s *MockFTPServer) Stop() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

func (s *MockFTPServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	write := func(code int, msg string) {
		response := fmt.Sprintf("%d %s\r\n", code, msg)
		writer.WriteString(response)
		writer.Flush()
	}

	write(220, "Service ready for new user.")

	var pasvListener net.Listener
	var pasvConn net.Conn
	var cwd = "/"
	var renameFrom string
	var restOffset int64

	defer func() {
		if pasvListener != nil {
			pasvListener.Close()
		}
		if pasvConn != nil {
			pasvConn.Close()
		}
	}()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch cmd {
		case "USER":
			write(331, "User name okay, need password.")
		case "PASS":
			write(230, "User logged in, proceed.")
		case "SYST":
			write(215, "UNIX Type: L8")
		case "FEAT":
			// Multiline response for FEAT
			fmt.Fprintf(writer, "211-Extensions supported:\r\n")
			fmt.Fprintf(writer, " SIZE\r\n")
			fmt.Fprintf(writer, " MDTM\r\n")
			fmt.Fprintf(writer, "211 END\r\n")
			writer.Flush()
		case "TYPE":
			write(200, "Type set to "+arg)
		case "EPSV":
			write(502, "Command not implemented.") // Force PASV
		case "PASV":
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				write(425, "Can't open passive connection")
				continue
			}
			pasvListener = l
			port := l.Addr().(*net.TCPAddr).Port
			p1 := port / 256
			p2 := port % 256
			write(227, fmt.Sprintf("Entering Passive Mode (127,0,0,1,%d,%d).", p1, p2))
		case "SIZE":
			path := filepath.Join(s.rootDir, arg)
			info, err := os.Stat(path)
			if err != nil {
				write(550, "File not found")
			} else {
				write(213, fmt.Sprintf("%d", info.Size()))
			}
		case "MDTM":
			path := filepath.Join(s.rootDir, arg)
			info, err := os.Stat(path)
			if err != nil {
				write(550, "File not found")
			} else {
				write(213, info.ModTime().Format("20060102150405"))
			}
		case "CWD":
			cwd = filepath.Join(cwd, arg)
			write(250, "Directory changed to "+cwd)
		case "PWD":
			write(257, fmt.Sprintf("\"%s\" is the current directory", cwd))
		case "MKD":
			path := filepath.Join(s.rootDir, arg)
			if err := os.MkdirAll(path, 0755); err != nil {
				write(550, "Failed to create directory")
			} else {
				write(257, fmt.Sprintf("\"%s\" created", arg))
			}
		case "RMD":
			path := filepath.Join(s.rootDir, arg)
			if err := os.RemoveAll(path); err != nil {
				write(550, "Failed to remove directory")
			} else {
				write(250, "Directory removed")
			}
		case "DELE":
			path := filepath.Join(s.rootDir, arg)
			if err := os.Remove(path); err != nil {
				write(550, "Failed to delete file")
			} else {
				write(250, "File deleted")
			}
		case "RNFR":
			renameFrom = filepath.Join(s.rootDir, arg)
			if _, err := os.Stat(renameFrom); err != nil {
				write(550, "File not found")
			} else {
				write(350, "Ready for RNTO")
			}
		case "RNTO":
			if renameFrom == "" {
				write(503, "Bad sequence of commands")
			} else {
				renameTo := filepath.Join(s.rootDir, arg)
				if err := os.Rename(renameFrom, renameTo); err != nil {
					write(550, "Rename failed")
				} else {
					write(250, "Rename successful")
				}
				renameFrom = ""
			}
		case "REST":
			// Set restart offset for next RETR command
			offset := int64(0)
			if _, err := fmt.Sscanf(arg, "%d", &offset); err == nil {
				restOffset = offset
				write(350, fmt.Sprintf("Restarting at %d", offset))
			} else {
				write(501, "Invalid offset")
			}
		case "RETR":
			if pasvListener == nil {
				write(425, "Use PASV or PORT first.")
				continue
			}
			path := filepath.Join(s.rootDir, arg)
			file, err := os.Open(path)
			if err != nil {
				pasvListener.Close()
				pasvListener = nil
				write(550, "File not found")
				continue
			}
			// Seek to offset if REST was used
			if restOffset > 0 {
				if _, err := file.Seek(restOffset, io.SeekStart); err != nil {
					file.Close()
					pasvListener.Close()
					pasvListener = nil
					write(550, "Failed to seek to offset")
					continue
				}
			}
			write(150, "Opening data connection")
			conn, err := pasvListener.Accept()
			pasvListener.Close()
			pasvListener = nil
			if err != nil {
				file.Close()
				write(425, "Data connection failed")
				continue
			}
			io.Copy(conn, file)
			conn.Close()
			file.Close()
			restOffset = 0 // Reset offset after use
			write(226, "Transfer complete")
		case "STOR":
			if pasvListener == nil {
				write(425, "Use PASV or PORT first.")
				continue
			}
			path := filepath.Join(s.rootDir, arg)
			// Ensure directory exists
			os.MkdirAll(filepath.Dir(path), 0755)
			file, err := os.Create(path)
			if err != nil {
				pasvListener.Close()
				pasvListener = nil
				write(550, "Failed to create file")
				continue
			}
			write(150, "Opening data connection")
			conn, err := pasvListener.Accept()
			pasvListener.Close()
			pasvListener = nil
			if err != nil {
				file.Close()
				write(425, "Data connection failed")
				continue
			}
			io.Copy(file, conn)
			conn.Close()
			file.Close()
			write(226, "Transfer complete")
		case "APPE":
			if pasvListener == nil {
				write(425, "Use PASV or PORT first.")
				continue
			}
			path := filepath.Join(s.rootDir, arg)
			// Ensure directory exists
			os.MkdirAll(filepath.Dir(path), 0755)
			file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				pasvListener.Close()
				pasvListener = nil
				write(550, "Failed to open file")
				continue
			}
			write(150, "Opening data connection")
			conn, err := pasvListener.Accept()
			pasvListener.Close()
			pasvListener = nil
			if err != nil {
				file.Close()
				write(425, "Data connection failed")
				continue
			}
			io.Copy(file, conn)
			conn.Close()
			file.Close()
			write(226, "Transfer complete")
		case "QUIT":
			write(221, "Goodbye.")
			return
		case "NOOP":
			write(200, "OK")
		default:
			write(502, "Command not implemented")
		}
	}
}
