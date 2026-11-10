package mux

import (
	"context"
	"io"
	"net"
	"time"
)

var bootstrap = [12]byte{'H', 'N', 'E', 'T', 0, 1, 0, 1, 0, 0, 0, 0}

func handshake(ctx context.Context, conn net.Conn, role Role, cfg Config) (settings, error) {
	deadline := time.Now().Add(cfg.IOTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return settings{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	readPreface := func() error {
		var b [12]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		if b != bootstrap {
			return protocolError("unsupported bootstrap")
		}
		return nil
	}
	sendSettings := func() error {
		b := cfg.settings().encode()
		return writeFrame(conn, header{kind: typeSettings, length: 24}, b[:])
	}
	var peer settings
	var err error
	if role == Initiator {
		if err = writeFull(conn, bootstrap[:]); err == nil {
			err = readPreface()
		}
		if err == nil {
			err = sendSettings()
		}
		if err == nil {
			peer, err = readSettings(conn)
		}
	} else {
		if err = readPreface(); err == nil {
			err = writeFull(conn, bootstrap[:])
		}
		if err == nil {
			peer, err = readSettings(conn)
		}
		if err == nil {
			err = sendSettings()
		}
	}
	stop()
	if ctx.Err() != nil {
		return settings{}, ctx.Err()
	}
	if err != nil {
		return settings{}, err
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return settings{}, err
	}
	return peer, nil
}
