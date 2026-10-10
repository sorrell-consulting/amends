package showcase

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type commitWireLoss struct {
	armed     atomic.Int32
	fired     atomic.Bool
	committed atomic.Bool
	dials     atomic.Int32
}

type commitWireConn struct {
	net.Conn
	loss      *commitWireLoss
	dropReply atomic.Bool
}

func ambiguousConfig(databaseURL string) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("ambiguous-commit requires a valid local PostgreSQL configuration")
	}
	host := config.ConnConfig.Host
	if config.ConnConfig.TLSConfig != nil || len(config.ConnConfig.Fallbacks) != 0 || (host != "localhost" && !net.ParseIP(host).IsLoopback()) {
		return nil, fmt.Errorf("ambiguous-commit requires one loopback TCP PostgreSQL endpoint with sslmode=disable, without TLS or host fallbacks")
	}
	config.MaxConns, config.MinConns = 1, 0
	return config, nil
}

func (loss *commitWireLoss) configure(config *pgxpool.Config) {
	dial := config.ConnConfig.DialFunc
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("commit fault refuses a non-loopback connection")
		}
		connection, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		loss.dials.Add(1)
		return &commitWireConn{Conn: connection, loss: loss}, nil
	}
}

func (connection *commitWireConn) Write(data []byte) (int, error) {
	if len(data) >= 5 && data[0] == 'Q' && uint64(binary.BigEndian.Uint32(data[1:5]))+1 == uint64(len(data)) && string(data[5:]) == "commit\x00" {
		switch connection.loss.armed.Swap(0) {
		case 1:
			connection.loss.fired.Store(true)
			connection.dropReply.Store(true)
		case 2:
			connection.loss.fired.Store(true)
			_ = connection.Conn.Close()
			return 0, io.ErrClosedPipe
		}
	}
	return connection.Conn.Write(data)
}

func (connection *commitWireConn) Read(data []byte) (int, error) {
	count, err := connection.Conn.Read(data)
	if !connection.dropReply.Swap(false) {
		return count, err
	}
	defer connection.Conn.Close()
	if err := consumeCommitReply(io.MultiReader(bytes.NewReader(data[:count]), connection.Conn)); err != nil {
		return 0, err
	}
	connection.loss.committed.Store(true)
	return 0, io.EOF
}

func consumeCommitReply(reader io.Reader) error {
	committed := false
	for {
		var header [5]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(header[1:])
		if length < 4 || length > 1<<20 {
			return fmt.Errorf("invalid PostgreSQL commit response length")
		}
		body := make([]byte, int(length)-4)
		if _, err := io.ReadFull(reader, body); err != nil {
			return err
		}
		switch header[0] {
		case 'C':
			if committed || string(body) != "COMMIT\x00" {
				return fmt.Errorf("unexpected PostgreSQL commit command tag")
			}
			committed = true
		case 'Z':
			if !committed || string(body) != "I" {
				return fmt.Errorf("PostgreSQL did not confirm a completed COMMIT")
			}
			return nil
		case 'E':
			return fmt.Errorf("PostgreSQL rejected the selected COMMIT")
		case 'N', 'S', 'A':
		default:
			return fmt.Errorf("unexpected PostgreSQL commit response type %q", header[0])
		}
	}
}
