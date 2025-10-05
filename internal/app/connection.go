package app

import (
	"bufio"
	"fmt"
	"strings"
	"time"
)

// readLineWithTimeout reads a line with timeout and better error handling
func (s *Subscriber) readLineWithTimeout(timeout time.Duration) (string, error) {
	if s.Connection == nil {
		return "", fmt.Errorf("connection is nil")
	}

	// Set read deadline
	s.Connection.SetReadDeadline(time.Now().Add(timeout))
	defer s.Connection.SetReadDeadline(time.Time{}) // Reset deadline

	line, err := s.Reader.ReadBytes('\n')
	if err == nil {
		return string(line), nil
	}

	if strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds") {
		fmt.Println("Buffer overflow detected, switching to direct connection reading")
		return s.readLineDirectly(timeout)
	}

	return "", err
}

// readLineDirectly reads directly from connection for very long lines
func (s *Subscriber) readLineDirectly(timeout time.Duration) (string, error) {
	s.Connection.SetReadDeadline(time.Now().Add(timeout))
	defer s.Connection.SetReadDeadline(time.Time{})

	var result []byte
	var buffer [1]byte

	for {
		n, err := s.Connection.Read(buffer[:])
		if err != nil {
			return "", err
		}

		if n > 0 {
			result = append(result, buffer[0])
			if buffer[0] == '\n' {
				break
			}
			// Safety check - prevent extremely long lines (10MB limit)
			if len(result) > 10*1024*1024 {
				return "", fmt.Errorf("line too long (>10MB), possibly corrupted connection")
			}
		}
	}

	return string(result), nil
}

// readLineWithScanner reads using Scanner (more robust for very long lines)
func (s *Subscriber) readLineWithScanner(timeout time.Duration) (string, error) {
	if s.Connection == nil {
		return "", fmt.Errorf("connection is nil")
	}

	s.Connection.SetReadDeadline(time.Now().Add(timeout))
	defer s.Connection.SetReadDeadline(time.Time{})

	// Create a new scanner directly on the connection
	scanner := bufio.NewScanner(s.Connection)

	// Increase the scanner buffer size for very long lines
	buf := make([]byte, 1024*1024)    // 1MB buffer
	scanner.Buffer(buf, 10*1024*1024) // Max token size 10MB

	if scanner.Scan() {
		return scanner.Text() + "\n", nil
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", fmt.Errorf("no line read")
}

// readLineWithTimeoutRobust reads a line with timeout and handles buffer overflows more gracefully
func (s *Subscriber) readLineWithTimeoutRobust(timeout time.Duration) (string, error) {
	if s.Connection == nil {
		return "", fmt.Errorf("connection is nil")
	}

	// Set read deadline
	s.Connection.SetReadDeadline(time.Now().Add(timeout))
	defer s.Connection.SetReadDeadline(time.Time{}) // Reset deadline

	// First try with the current reader
	line, err := s.Reader.ReadBytes('\n')
	if err == nil {
		return string(line), nil
	}

	// If we get a buffer overflow, try different strategies
	if strings.Contains(err.Error(), "buffer") || strings.Contains(err.Error(), "slice bounds") {
		fmt.Printf("Buffer overflow detected, trying alternative reading methods: %v\n", err)

		// Strategy 1: Reset reader with larger buffer
		s.Reader = bufio.NewReaderSize(s.Connection, 4*1024*1024) // 4MB buffer
		line, err = s.Reader.ReadBytes('\n')
		if err == nil {
			return string(line), nil
		}

		// Strategy 2: Use scanner with large buffer
		result, err := s.readLineWithScanner(timeout)
		if err == nil {
			return result, nil
		}

		// Strategy 3: Read directly from connection
		fmt.Println("Falling back to direct connection reading")
		return s.readLineDirectly(timeout)
	}

	return "", err
}
