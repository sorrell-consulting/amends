//go:build integration

package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/store/postgres"
)

type delayedDisconnectConn struct {
	net.Conn
	held           atomic.Bool
	terminateHeld  atomic.Bool
	closeRequested atomic.Bool
}

func (connection *delayedDisconnectConn) Write(data []byte) (int, error) {
	if connection.held.Load() && bytes.Equal(data, []byte{'X', 0, 0, 0, 4}) {
		connection.terminateHeld.Store(true)
		return len(data), nil
	}
	return connection.Conn.Write(data)
}

func (connection *delayedDisconnectConn) Close() error {
	if connection.held.Load() {
		connection.closeRequested.Store(true)
		return nil
	}
	return connection.Conn.Close()
}

type producerDisconnect struct {
	key       int64
	candidate atomic.Pointer[delayedDisconnectConn]
	selected  atomic.Pointer[delayedDisconnectConn]
}

func (delay *producerDisconnect) TraceQueryStart(ctx context.Context, connection *pgx.Conn, query pgx.TraceQueryStartData) context.Context {
	socket, ok := connection.PgConn().Conn().(*delayedDisconnectConn)
	if !ok {
		return ctx
	}
	if query.SQL == "SELECT pg_try_advisory_lock($1)" && len(query.Args) == 1 && query.Args[0] == delay.key {
		delay.candidate.Store(socket)
	}
	if query.SQL == "SELECT 1" && delay.candidate.Load() == socket {
		socket.held.Store(true)
		delay.selected.Store(socket)
	}
	return ctx
}

func (*producerDisconnect) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (delay *producerDisconnect) release() error {
	if socket := delay.selected.Swap(nil); socket != nil {
		socket.held.Store(false)
		return socket.Conn.Close()
	}
	return nil
}

func delayedProducerDisconnect(test *testing.T, environment *environment) (*postgres.Store, *producerDisconnect) {
	test.Helper()
	config := environment.pool.Config()
	if config.ConnConfig.TLSConfig != nil {
		test.Fatal("disconnect-boundary test requires the local sslmode=disable endpoint")
	}
	sum := sha256.Sum256([]byte(environment.Manifest.Config.Source + "/producer"))
	delay := &producerDisconnect{key: int64(binary.BigEndian.Uint64(sum[:8]))}
	config.ConnConfig.Tracer = delay
	dial := config.ConnConfig.DialFunc
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &delayedDisconnectConn{Conn: connection}, nil
	}
	pool, err := pgxpool.NewWithConfig(test.Context(), config)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(pool.Close)
	test.Cleanup(func() {
		if err := delay.release(); err != nil {
			test.Error(err)
		}
	})
	state, err := postgres.New(pool, environment.Manifest.Config)
	if err != nil {
		test.Fatal(err)
	}
	return state, delay
}
