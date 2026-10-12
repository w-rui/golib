package net

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// Each fixture owns one loopback connection and reports when the peer closes it.
func dialProxyFixture(t *testing.T, kind string, stall bool) (string, <-chan struct{}, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		t.Cleanup(func() { c.Close() })
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(c)
		if kind == "http" {
			req, e := http.ReadRequest(reader)
			if e != nil {
				done <- e
				return
			}
			if req.Method != "CONNECT" || req.Host != "target.example:7000" || req.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:password")) {
				done <- fmt.Errorf("invalid CONNECT: %+v", req)
				return
			}
			close(ready)
			if !stall {
				_, err = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
			}
		} else {
			// RFC 1928 greeting and RFC 1929 username/password negotiation.
			header := make([]byte, 2)
			if _, err = io.ReadFull(reader, header); err != nil {
				done <- err
				return
			}
			methods := make([]byte, int(header[1]))
			if _, err = io.ReadFull(reader, methods); err != nil {
				done <- err
				return
			}
			close(ready)
			if stall {
				_, err = io.Copy(io.Discard, reader)
				done <- err
				return
			}
			_, err = c.Write([]byte{5, 2})
			if err != nil {
				done <- err
				return
			}
			if _, err = io.ReadFull(reader, header); err != nil {
				done <- err
				return
			}
			user := make([]byte, int(header[1]))
			if _, err = io.ReadFull(reader, user); err != nil {
				done <- err
				return
			}
			length, e := reader.ReadByte()
			if e != nil {
				done <- e
				return
			}
			password := make([]byte, int(length))
			if _, err = io.ReadFull(reader, password); err != nil {
				done <- err
				return
			}
			if header[0] != 1 || string(user) != "alice" || string(password) != "password" {
				done <- fmt.Errorf("invalid SOCKS credentials")
				return
			}
			_, err = c.Write([]byte{1, 0})
			if err != nil {
				done <- err
				return
			}
			request := make([]byte, 5)
			if _, err = io.ReadFull(reader, request); err != nil {
				done <- err
				return
			}
			target := make([]byte, int(request[4])+2)
			if _, err = io.ReadFull(reader, target); err != nil {
				done <- err
				return
			}
			if string(request[:4]) != string([]byte{5, 1, 0, 3}) || string(target[:len(target)-2]) != "target.example" || target[len(target)-2] != 27 || target[len(target)-1] != 88 {
				done <- fmt.Errorf("invalid SOCKS target: %v %v", request, target)
				return
			}
			_, err = c.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
		}
		if err != nil {
			done <- err
			return
		}
		if stall {
			_, err = io.Copy(io.Discard, reader)
		} else {
			_, err = io.Copy(c, reader)
		}
		done <- err
	}()
	return ln.Addr().String(), ready, done
}

func TestDialContextProxyCancellation(t *testing.T) {
	for _, kind := range []string{"http", "socks5"} {
		for _, mode := range []string{"cancel", "deadline", "timeout"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				address, ready, closed := dialProxyFixture(t, kind, true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				timeout := time.Second
				if mode == "deadline" {
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				}
				if mode == "timeout" {
					timeout = 100 * time.Millisecond
				}
				result := make(chan error, 1)
				go func() {
					c, err := DialContext(ctx, "target.example:7000", WithProxy(kind, address), WithProxyAuth(&ProxyAuth{Username: "alice", Passwd: "password"}), WithTimeout(timeout))
					if c != nil {
						c.Close()
					}
					result <- err
				}()
				select {
				case <-ready:
				case err := <-closed:
					t.Fatalf("fixture failed: %v", err)
				case <-time.After(time.Second):
					t.Fatal("proxy handshake not reached")
				}
				if mode == "cancel" {
					cancel()
				}
				select {
				case err := <-result:
					want := context.DeadlineExceeded
					if mode == "cancel" {
						want = context.Canceled
					}
					if !errors.Is(err, want) {
						t.Fatalf("error = %v, want %v", err, want)
					}
				case <-time.After(500 * time.Millisecond):
					t.Fatal("dial goroutine retained after cancellation/deadline")
				}
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("socket not cleanly released: %v", err)
					}
				case <-time.After(500 * time.Millisecond):
					t.Fatal("proxy socket retained")
				}
			})
		}
	}
}

func TestDialContextProxyStreamSurvivesDialDeadline(t *testing.T) {
	for _, kind := range []string{"http", "socks5"} {
		t.Run(kind, func(t *testing.T) {
			address, _, done := dialProxyFixture(t, kind, false)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			c, err := DialContext(ctx, "target.example:7000", WithProxy(kind, address), WithProxyAuth(&ProxyAuth{Username: "alice", Passwd: "password"}), WithTimeout(100*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			<-ctx.Done()
			// Cancellation of the completed dial must not close the returned stream or
			// leave a handshake deadline that breaks subsequent traffic.
			payload := []byte{0, 255, 128}
			if _, err = c.Write(payload); err != nil {
				t.Fatal(err)
			}
			// Set only a read deadline: a retained expired write deadline remains visible.
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			echoed := make([]byte, len(payload))
			if _, err = io.ReadFull(c, echoed); err != nil {
				t.Fatal(err)
			}
			if string(echoed) != string(payload) {
				t.Fatalf("echo = %v", echoed)
			}
			c.Close()
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDialContextCustomDialer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctxKey := struct{}{}
	ctx := context.WithValue(context.Background(), ctxKey, "custom-dial")
	c, err := DialContext(ctx, "example.invalid:7000", WithDialer(func(ctx context.Context, destination string) (net.Conn, error) {
		if ctx.Value(ctxKey) != "custom-dial" || destination != "example.invalid:7000" {
			return nil, fmt.Errorf("raw dial arguments not preserved")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted.Close()
}

func TestDialContextHookErrorReleasesSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, err = io.Copy(io.Discard, c)
		done <- err
	}()
	want := errors.New("hook rejected connection")
	_, err = DialContext(context.Background(), ln.Addr().String(), WithAfterHook(AfterHook{Hook: func(context.Context, net.Conn, string) (context.Context, net.Conn, error) {
		return nil, nil, want
	}}))
	if !errors.Is(err, want) {
		t.Fatalf("hook error = %v", err)
	}
	if err = <-done; err != nil {
		t.Fatalf("raw socket retained: %v", err)
	}
}
