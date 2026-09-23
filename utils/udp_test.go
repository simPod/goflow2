package utils

import (
	"net"
	"strconv"
	"testing"

	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDPReceiver(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)
	t.Logf("starting UDP receiver on %s:%d\n", addr, port)

	r, err := NewUDPReceiver(nil)
	require.NoError(t, err)

	require.NoError(t, r.Start(addr, port, nil))
	sendMessage := func(msg string) error {
		conn, err := net.Dial("udp", net.JoinHostPort(addr, strconv.Itoa(port)))
		if err != nil {
			return err
		}
		_, err = conn.Write([]byte(msg))
		if err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return closeErr
			}
			return err
		}
		if err := conn.Close(); err != nil {
			return err
		}
		return nil
	}
	require.NoError(t, sendMessage("message"))
	t.Log("sending message\n")
	require.NoError(t, r.Stop())
}

func TestUDPClose(t *testing.T) {
	addr := "::1"
	port, err := getFreeUDPPort()
	require.NoError(t, err)
	t.Logf("starting UDP receiver on %s:%d\n", addr, port)

	r, err := NewUDPReceiver(nil)
	require.NoError(t, err)
	require.NoError(t, r.Start(addr, port, nil))
	require.NoError(t, r.Stop())
	require.NoError(t, r.Start(addr, port, nil))
	require.Error(t, r.Start(addr, port, nil))
	require.NoError(t, r.Stop())
	require.Error(t, r.Stop())
}

// The socket-close watcher may still be scheduled as Stop resets receiver state.
// Exercise traffic-driven exits as well as exits caused by closing the socket.
func TestUDPReceiverRapidRestart(t *testing.T) {
	port, err := getFreeUDPPort()
	require.NoError(t, err)
	r, err := NewUDPReceiver(&UDPReceiverConfig{Workers: 2, Sockets: 2, QueueSize: 32})
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, r.Start("127.0.0.1", port, nil))
		conn, err := net.Dial("udp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		require.NoError(t, err)
		_, err = conn.Write([]byte("restart"))
		require.NoError(t, conn.Close())
		require.NoError(t, err)
		require.NoError(t, r.Stop())
	}
}

func getFreeUDPPort() (int, error) {
	a, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenUDP("udp", a)
	if err != nil {
		return 0, err
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}
