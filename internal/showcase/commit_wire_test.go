package showcase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"testing/iotest"
	"time"
)

func commitFrame(kind byte, body string) []byte {
	frame := make([]byte, 5, 5+len(body))
	frame[0] = kind
	binary.BigEndian.PutUint32(frame[1:], uint32(4+len(body)))
	return append(frame, body...)
}

func TestCommitReplyRequiresCompleteServerConfirmation(test *testing.T) {
	complete := append(commitFrame('C', "COMMIT\x00"), commitFrame('Z', "I")...)
	for _, scenario := range []struct {
		name  string
		bytes []byte
		valid bool
	}{
		{"complete", complete, true},
		{"notice", append(commitFrame('N', "notice\x00"), complete...), true},
		{"missing ready", commitFrame('C', "COMMIT\x00"), false},
		{"missing tag", commitFrame('Z', "I"), false},
		{"rollback", append(commitFrame('C', "ROLLBACK\x00"), commitFrame('Z', "I")...), false},
		{"server error", commitFrame('E', "error\x00"), false},
		{"transaction active", append(commitFrame('C', "COMMIT\x00"), commitFrame('Z', "T")...), false},
		{"duplicate tag", append(commitFrame('C', "COMMIT\x00"), complete...), false},
		{"invalid type", commitFrame('X', ""), false},
		{"short length", []byte{'C', 0, 0, 0, 3}, false},
		{"oversized", []byte{'C', 0x7f, 0xff, 0xff, 0xff}, false},
		{"partial header", []byte{'C', 0}, false},
		{"partial body", complete[:9], false},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			err := consumeCommitReply(iotest.OneByteReader(bytes.NewReader(scenario.bytes)))
			if (err == nil) != scenario.valid {
				test.Fatalf("invalid COMMIT confirmation acceptance: %v", err)
			}
		})
	}
}

func TestCommitWireLoss(test *testing.T) {
	for _, mode := range []int32{1, 2} {
		name := "reply"
		if mode == 2 {
			name = "request"
		}
		test.Run(name, func(test *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				test.Fatal(err)
			}
			loss := &commitWireLoss{}
			loss.armed.Store(mode)
			connection := &commitWireConn{Conn: client, loss: loss}
			request := commitFrame('Q', "commit\x00")
			serverResult := make(chan error, 1)
			go func() {
				actual := make([]byte, len(request))
				count, err := io.ReadFull(server, actual)
				if mode == 2 {
					if count == 0 && errors.Is(err, io.EOF) {
						serverResult <- nil
					} else {
						serverResult <- errors.New("lost request reached the server")
					}
					return
				}
				if err != nil || !bytes.Equal(actual, request) {
					serverResult <- errors.New("request did not reach the server intact")
					return
				}
				_, err = server.Write(append(commitFrame('C', "COMMIT\x00"), commitFrame('Z', "I")...))
				serverResult <- err
			}()
			count, err := connection.Write(request)
			if mode == 2 {
				if count != 0 || !errors.Is(err, io.ErrClosedPipe) {
					test.Fatalf("request was not dropped: %d %v", count, err)
				}
			} else {
				if err != nil || count != len(request) {
					test.Fatal("request write", err)
				}
				var buffer [1024]byte
				if count, err = connection.Read(buffer[:]); count != 0 || !errors.Is(err, io.EOF) {
					test.Fatal("successful COMMIT reply escaped to caller", count, err)
				}
			}
			if err = <-serverResult; err != nil {
				test.Fatal(err)
			}
			if !loss.fired.Load() || loss.armed.Load() != 0 || loss.committed.Load() != (mode == 1) {
				test.Fatal("wire evidence differs from observed boundary")
			}
		})
	}
}

func TestAmbiguousCommitEndpointPreflight(test *testing.T) {
	for _, endpoint := range []string{
		"postgres://localhost/amends?sslmode=disable",
		"postgres://127.0.0.1/amends?sslmode=disable",
		"postgres://[::1]/amends?sslmode=disable",
	} {
		config, err := ambiguousConfig(endpoint)
		if err != nil || config.MaxConns != 1 || config.MinConns != 0 {
			test.Fatal("local endpoint rejected", err)
		}
	}
	for _, endpoint := range []string{
		"postgres://127.0.0.1/amends?sslmode=require",
		"postgres://127.0.0.1/amends?sslmode=prefer",
		"postgres://192.0.2.1/amends?sslmode=disable",
		"postgres://example.invalid/amends?sslmode=disable",
		"host=/tmp dbname=amends sslmode=disable",
		"host=127.0.0.1,127.0.0.2 dbname=amends sslmode=disable",
		"invalid",
	} {
		options := Options{DatabaseURL: endpoint, Brokers: []string{"unused"}, ControlBinary: "unused", WorkerBinary: "unused", ArtifactParent: test.TempDir(), Scenario: AmbiguousReply}
		if report, err := Run(test.Context(), options); err == nil || report.Directory != "" {
			test.Fatalf("unsupported endpoint reached setup: %q report=%+v error=%v", endpoint, report, err)
		}
	}
}

type commitReadStarted struct {
	net.Conn
	started chan struct{}
}

func (connection *commitReadStarted) Read(data []byte) (int, error) {
	select {
	case <-connection.started:
	default:
		close(connection.started)
	}
	return connection.Conn.Read(data)
}

func TestCommitReplyLossWithReadAlreadyWaiting(test *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		test.Fatal(err)
	}
	started := &commitReadStarted{Conn: client, started: make(chan struct{})}
	loss := &commitWireLoss{}
	loss.armed.Store(1)
	connection := &commitWireConn{Conn: started, loss: loss}
	result := make(chan error, 1)
	go func() {
		var buffer [1024]byte
		count, err := connection.Read(buffer[:])
		if count != 0 || !errors.Is(err, io.EOF) {
			result <- errors.New("reply escaped through an already waiting reader")
		} else {
			result <- nil
		}
	}()
	<-started.started
	request := commitFrame('Q', "commit\x00")
	serverResult := make(chan error, 1)
	go func() {
		if _, err := io.ReadFull(server, make([]byte, len(request))); err != nil {
			serverResult <- err
			return
		}
		_, err := server.Write(append(commitFrame('C', "COMMIT\x00"), commitFrame('Z', "I")...))
		serverResult <- err
	}()
	if _, err := connection.Write(request); err != nil {
		test.Fatal(err)
	}
	if err := <-result; err != nil {
		test.Fatal(err)
	}
	if err := <-serverResult; err != nil || !loss.committed.Load() {
		test.Fatal("server confirmation was not retained", err)
	}
}

func TestCommitLossIgnoresOtherWrites(test *testing.T) {
	for _, request := range [][]byte{
		commitFrame('Q', "begin\x00"), commitFrame('Q', "rollback\x00"),
		commitFrame('Q', "select 'commit'\x00"), commitFrame('P', "commit\x00"),
		{'Q', 0, 0, 0, 4, 'c', 'o', 'm', 'm', 'i', 't', 0},
	} {
		client, server := net.Pipe()
		if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			test.Fatal(err)
		}
		loss := &commitWireLoss{}
		loss.armed.Store(2)
		connection := &commitWireConn{Conn: client, loss: loss}
		result := make(chan error, 1)
		go func() {
			actual := make([]byte, len(request))
			_, err := io.ReadFull(server, actual)
			if err == nil && !bytes.Equal(actual, request) {
				err = errors.New("unselected write was changed")
			}
			result <- err
		}()
		count, writeErr := connection.Write(request)
		readErr := <-result
		client.Close()
		server.Close()
		if count != len(request) || writeErr != nil || readErr != nil || loss.fired.Load() || loss.armed.Load() != 2 {
			test.Fatal("fault affected an unselected write", count, writeErr, readErr)
		}
	}
}
