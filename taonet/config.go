package taonet

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const muxSendBudget = 1 << 20

func validName(s string) bool {
	return len(s) > 0 && len(s) <= 255 && utf8.ValidString(s) && !strings.ContainsFunc(s, func(r rune) bool { return r < 32 || r == 127 })
}

func (l Limits) normalized() (Limits, error) {
	if l.ChannelMaxMessage == 0 {
		l.ChannelMaxMessage = 256 << 10
	}
	if l.ChannelWindowBytes == 0 {
		l.ChannelWindowBytes = 1 << 20
	}
	if l.ChannelWindowMessages == 0 {
		l.ChannelWindowMessages = 256
	}
	if l.ChannelMaxMessage < 1 || l.ChannelMaxMessage > 4<<20 || l.ChannelWindowBytes < l.ChannelMaxMessage+128 || l.ChannelWindowBytes > 64<<20 || l.ChannelWindowMessages < 1 || l.ChannelWindowMessages > 4096 {
		return Limits{}, fmt.Errorf("taonet: invalid channel limits")
	}
	if l.MaxMessageSize == 0 {
		l.MaxMessageSize = 4 << 20
	}
	if l.MaxMetadataBytes == 0 {
		l.MaxMetadataBytes = 8 << 10
	}
	if l.MaxMetadataEntries == 0 {
		l.MaxMetadataEntries = 32
	}
	if l.MaxOperations == 0 {
		l.MaxOperations = 256
	}
	if l.MaxHandlers == 0 {
		l.MaxHandlers = min(64, l.MaxOperations)
	}
	if l.ReceiveBytes == 0 {
		l.ReceiveBytes = 16 << 20
	}
	if l.SendBytes == 0 {
		l.SendBytes = 8 << 20
	}
	if l.MaxMessageSize < 1 || l.MaxMessageSize > 64<<20 || l.MaxMetadataBytes < 1 || l.MaxMetadataBytes > 8<<10 ||
		l.MaxMetadataEntries < 1 || l.MaxMetadataEntries > 256 || l.MaxOperations < 1 || l.MaxOperations > 4096 ||
		l.MaxHandlers < 1 || l.MaxHandlers > l.MaxOperations || l.ReceiveBytes < l.MaxMessageSize || l.SendBytes < l.MaxMessageSize+muxSendBudget {
		return Limits{}, fmt.Errorf("taonet: invalid limits")
	}
	return l, nil
}

func (c SessionConfig) normalized() (SessionConfig, error) {
	if c.Local.Namespace == "" {
		c.Local.Namespace = "default"
	}
	if !validName(string(c.Local.ID)) || !validName(c.Local.Namespace) {
		return SessionConfig{}, fmt.Errorf("taonet: invalid local identity")
	}
	var err error
	if c.Local.Instance == (ID{}) {
		c.Local.Instance, err = processInstance()
		if err != nil {
			return SessionConfig{}, err
		}
	}
	c.Limits, err = c.Limits.normalized()
	if err != nil {
		return SessionConfig{}, err
	}
	if c.StreamReceiveWindow == 0 {
		c.StreamReceiveWindow = 64 << 10
	}
	if c.StreamReceiveWindow < 4096 || c.StreamReceiveWindow > 1<<20 || c.StreamReceiveWindow%4096 != 0 {
		return SessionConfig{}, fmt.Errorf("taonet: invalid stream receive window")
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.DefaultRequestTimeout == 0 {
		c.DefaultRequestTimeout = 5 * time.Second
	}
	if c.DefaultWriteTimeout == 0 {
		c.DefaultWriteTimeout = 5 * time.Second
	}
	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = 5 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 15 * time.Second
	}
	if c.HandshakeTimeout < 0 || c.DefaultRequestTimeout < 0 || c.DefaultWriteTimeout < 0 || c.KeepAliveInterval < 0 || c.IdleTimeout <= c.KeepAliveInterval {
		return SessionConfig{}, fmt.Errorf("taonet: invalid timeout")
	}
	if c.Mux == nil {
		c.Mux = NewMux()
	}
	if c.Transport.TLS != nil {
		c.Transport.TLS = c.Transport.TLS.Clone()
	}
	return c, nil
}

func checkMetadata(meta map[string]string, limits Limits) error {
	if len(meta) == 0 {
		return nil
	}
	if len(meta) > limits.MaxMetadataEntries {
		return &StatusError{Code: CodeMessageTooLarge, Message: "too many metadata entries"}
	}
	n := 0
	for k, v := range meta {
		n += len(k) + len(v)
		if n > limits.MaxMetadataBytes {
			return &StatusError{Code: CodeMessageTooLarge, Message: "metadata too large"}
		}
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if len(encoded) > limits.MaxMetadataBytes {
		return &StatusError{Code: CodeMessageTooLarge, Message: "encoded metadata too large"}
	}
	return nil
}
