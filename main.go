package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/LiamHaworth/go-tproxy"
)

var (
	udpListener *net.UDPConn
)

type ConnIdent struct {
	srcAddr *net.UDPAddr
	dstAddr *net.UDPAddr
}

type UDPProxyConn struct {
	commChan     chan []byte
	proxyAddress string
	lastAccess   int64
}

type TTLMap struct {
	m    map[string]*UDPProxyConn
	lock sync.Mutex
	done chan bool
}

func NewTTLMap(ln int, maxTTL int) (m *TTLMap) {
	m = &TTLMap{
		m:    make(map[string]*UDPProxyConn, ln),
		done: make(chan bool),
	}
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case <-m.done:
				log.Printf("exit signal")
				return
			case now := <-ticker.C:
				m.lock.Lock()
				for k, v := range m.m {
					if now.Unix()-v.lastAccess > int64(maxTTL) {
						delete(m.m, k)
					}
				}
				m.lock.Unlock()
			}
		}
	}()
	return
}

func (m *TTLMap) Len() int {
	return len(m.m)
}

func (m *TTLMap) Put(k string, proxyConn *UDPProxyConn) {
	m.lock.Lock()
	_, ok := m.m[k]
	if !ok {
		m.m[k] = proxyConn
	}
	m.lock.Unlock()
}

func (m *TTLMap) Get(k string) (proxyConn *UDPProxyConn) {
	m.lock.Lock()
	if it, ok := m.m[k]; ok {
		it.lastAccess = time.Now().Unix()
		proxyConn = it
	}
	m.lock.Unlock()
	return proxyConn
}

func NewUDPProxyConn(bufsize int, proxyaddress string, connIdent *ConnIdent) (err error, proxyconn *UDPProxyConn) {
	proxyconn = &UDPProxyConn{
		proxyAddress: proxyaddress,
		commChan:     make(chan []byte, bufsize),
		lastAccess:   time.Now().Unix(),
	}
	return
}

// main will initialize the TProxy
// handling application
func main() {
	log.Println("Binding UDP TProxy listener to 0.0.0.0:8080")
	var err error

	config := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			var err error
			err = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			err = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1)
			})
			if err != nil {
				return err
			}
			err = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_RECVORIGDSTADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	lp, err := config.ListenPacket(context.Background(), "udp", ":8080")

	// udpListener, err = tproxy.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080})
	if err != nil {
		log.Fatalf("Encountered error while binding UDP listener: %s", err)
		return
	}
	udpListener := lp.(*net.UDPConn)
	log.Printf("udpListener: %v", udpListener)

	defer udpListener.Close()
	ttlmap := NewTTLMap(1024*32, 16)
	go listenUDP(ttlmap, udpListener)

	tcpListener, err := net.Listen("tcp", "0.0.0.0:8055")
	if err != nil {
		log.Fatalf("Error trying to listen: %s", err)
		return
	}
	defer tcpListener.Close()
	go TCPServer(tcpListener)

	interruptListener := make(chan os.Signal)
	signal.Notify(interruptListener, os.Interrupt)
	<-interruptListener
	ttlmap.done <- true

	log.Println("TProxy listener closing")
	os.Exit(0)
}

func TCPServer(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("Error durnig accept: %s", err)
			return
		}
		go handleTCPReq(conn)
	}
}

func handleTCPReq(proxyConn net.Conn) {
	defer proxyConn.Close()
	// Handshake
	buf := make([]byte, 8)
	_, err := proxyConn.Read(buf)
	if err != nil {
		log.Print("TCP Read error", err)
		return
	}
	// srcAddr := &net.UDPAddr{
	// 	IP: net.IPv4(buf[0], buf[1], buf[2], buf[3]),
	// 	Port: (int(buf[8]) << 8) + int(buf[9])}
	dstAddr := &net.UDPAddr{
		IP:   net.IPv4(buf[0], buf[1], buf[2], buf[3]),
		Port: (int(buf[4]) << 8) + int(buf[5])}
	log.Printf("< HANDSHAKE -> %s", dstAddr)

	remoteConn, err := net.DialUDP("udp", nil, dstAddr)
	if err != nil {
		log.Printf("Failed to connect to original UDP destination [%s]: %s", dstAddr, err)
		return
	}
	defer remoteConn.Close()

	log.Printf("Waiting new data for piping")
	var streamWait sync.WaitGroup
	streamWait.Add(2)

	streamComm := func(inf string, dst io.Writer, src io.Reader) {
		defer streamWait.Done()
		// io.Copy(dst, src)
		buf := make([]byte, 1448)
		for {
			nc, err := io.CopyBuffer(dst, src, buf)
			if err != nil {
				log.Println("[%s] Copy error (after %d bytes): %s", inf, nc, err)
				return
			}
		}
		// for {
		// 	nr, err := src.Read(buf)
		// 	if err != nil {
		// 		log.Printf("[%s] Read error: %s", inf, err)
		// 		return
		// 	}
		// 	nw, err := dst.Write(buf)
		// 	if err != nil {
		// 		log.Printf("[%s] Write error: %s", inf, err)
		// 		return
		// 	}
		// 	if nw != nr {
		// 		log.Printf("[%s] Read: %d bytes Written: %d bytes", inf, nw, nr)
		// 	}
		// }
	}

	go streamComm(
		fmt.Sprintf("%s -> %s", proxyConn.LocalAddr(), dstAddr),
		remoteConn, proxyConn)
	go streamComm(
		fmt.Sprintf("%s -> %s", dstAddr, proxyConn.LocalAddr()),
		proxyConn, remoteConn)

	streamWait.Wait()
}

// listenUDP runs in a routine to
// accept UDP connections and hand them
// off into their own routines for handling
func listenUDP(ttlmap *TTLMap, udpListener *net.UDPConn) {
	for {
		buf := make([]byte, 1500)
		log.Printf("Blocking read from udp %v", udpListener)
		n, srcAddr, dstAddr, err := tproxy.ReadFromUDP(udpListener, buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("Temporary error while reading data: %s", netErr)
			}

			log.Fatalf("Unrecoverable error while reading data: %s", err)
			return
		}
		log.Printf("n: %d %s -> %s", n, srcAddr, dstAddr)

		// Search if a TCP connection is already opened for the
		// UDP stream corresponding to the UDP packet
		data := make([]byte, n)
		copy(data, buf[:n])
		connIdent := &ConnIdent{srcAddr: srcAddr, dstAddr: dstAddr}
		connKey := fmt.Sprintf("%s_%s", srcAddr, dstAddr)
		connProxy := ttlmap.Get(connKey)
		if connProxy == nil {
			log.Printf("New UDP proxy connection %s -> %s", connIdent.srcAddr, connIdent.dstAddr)
			err, connProxy = NewUDPProxyConn(1000, "daboog.zehome.com:8055", connIdent)
			if err != nil {
				log.Printf("Error creating UDP Proxy connection: %s", err)
				return
			}
			ttlmap.Put(connKey, connProxy)
			connProxy.commChan <- data
			go handleUDPConn(connIdent, connProxy)
		} else {
			connProxy.commChan <- data
		}
		log.Printf("loop() done")
	}
}

func handleUDPConn(connIdent *ConnIdent, connProxy *UDPProxyConn) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", connProxy.proxyAddress)
	if err != nil {
		log.Fatalf("Unable to resolve %s: %s", connProxy.proxyAddress, err)
		return
	}
	remoteConn, err := net.DialTCP("tcp", nil, tcpaddr)
	if err != nil {
		log.Fatalf("Error trying to listen: %s", err)
		return
	}
	remoteConn.SetLinger(0)
	remoteConn.SetNoDelay(true)
	remoteConn.SetReadBuffer(65535)
	remoteConn.SetWriteBuffer(65535)
	//remoteConn.SetWriteDeadline(time.Now().Add(time.Second))
	defer remoteConn.Close()

	dialer := &net.Dialer{
		LocalAddr: connIdent.dstAddr,
		Timeout:   time.Second * 5,
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			var err error
			err = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			err = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	if err != nil {
		log.Printf("Failed to connect to original UDP source [%s]: %s", connIdent.srcAddr, err)
		return
	}
	localConn, err := dialer.Dial("udp", connIdent.srcAddr.String())
	if err != nil {
		log.Printf("DialUDP failed: %s", err)
		return
	}
	defer localConn.Close()

	// Initial handshake
	if err := sendHandshake(connIdent, remoteConn); err != nil {
		log.Printf("Handshake failed: %s", err)
		return
	}

	log.Printf("Entering copy loop")
	var streamWait sync.WaitGroup
	streamWait.Add(3)

	streamComm := func(dst io.Writer, src io.Reader) {
		defer streamWait.Done()
		buf := make([]byte, 1448)
		for {
			nc, err := io.CopyBuffer(dst, src, buf)
			if err != nil {
				log.Println("Copy error (After %d bytes): %s", nc, err)
				return
			}
		}
		// for {
		// 	nr, err := src.Read(buf)
		// 	if err != nil {
		// 		log.Printf("Read error: %s", err)
		// 		return
		// 	}
		// 	nw, err := dst.Write(buf)
		// 	if err != nil {
		// 		log.Printf("Write error: %s", err)
		// 		return
		// 	}
		// 	if nw != nr {
		// 		log.Printf("Read: %d bytes Written: %d bytes", nw, nr)
		// 	}
		// }
	}

	go streamComm(remoteConn, localConn)
	go streamComm(localConn, remoteConn)

	go func() {
		defer streamWait.Done()
		for {
			select {
			case msg := <-connProxy.commChan:
				_, err := remoteConn.Write(msg)
				if err != nil {
					log.Printf("Write error: %s", err)
					return
				}
			}
		}
	}()
	streamWait.Wait()
}

func sendHandshake(connIdent *ConnIdent, remoteConn net.Conn) (err error) {
	dstAddr := connIdent.dstAddr
	// IP SRC + IP DST + PORT SRC + PORT DST
	newpkt := []byte{
		dstAddr.IP[12],
		dstAddr.IP[13],
		dstAddr.IP[14],
		dstAddr.IP[15],
		byte(dstAddr.Port >> 8),
		byte(dstAddr.Port),
	}
	log.Printf("Sending handshake %d", len(newpkt))
	written, err := remoteConn.Write(newpkt)
	if err != nil {
		log.Printf("Proxy write error: %s", err)
	} else {
		log.Printf("> HDR %d bytes", written)
	}
	return
}
