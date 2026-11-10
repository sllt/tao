package taonet

// Only explicitly released bodies enter this cache. Exact-size reuse keeps
// the payload charge equal to its allocated capacity; cached bytes occupy
// spare receive credit, including while out-of-order decisions delay WINDOW.
func (c *Channel) takeReceiveBodyLocked(size int) []byte {
	var body []byte
	for i := len(c.receiveCache) - 1; i >= 0; i-- {
		if len(c.receiveCache[i]) == size {
			body = c.receiveCache[i]
			last := len(c.receiveCache) - 1
			c.receiveCache[i] = c.receiveCache[last]
			c.receiveCache[last] = nil
			c.receiveCache = c.receiveCache[:last]
			c.receiveCacheBytes -= uint64(size)
			break
		}
	}
	spare := c.local.Bytes - (c.receivedBytes - c.releasedBytes)
	for c.receiveCacheBytes > spare {
		last := len(c.receiveCache) - 1
		c.receiveCacheBytes -= uint64(len(c.receiveCache[last]))
		c.receiveCache[last] = nil
		c.receiveCache = c.receiveCache[:last]
	}
	if body == nil {
		body = make([]byte, size)
	}
	return body
}

func (c *Channel) cacheReceiveBodyLocked(body []byte) {
	if len(body) == 0 || len(c.receiveCache) >= 256 {
		return
	}
	spare := c.local.Bytes - (c.receivedBytes - c.releasedBytes)
	if uint64(len(body)) > spare-c.receiveCacheBytes {
		return
	}
	c.receiveCache = append(c.receiveCache, body)
	c.receiveCacheBytes += uint64(len(body))
}
