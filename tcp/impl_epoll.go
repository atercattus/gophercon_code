package main

import (
	"bytes"
	"log"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	EPOLLET        = 1 << 31 // в stdlib идет не того типа, как хотелось бы
	MaxEpollEvents = 64
	SO_REUSEPORT   = 15 // нет в stdlib
)

var (
	str11 = []byte(`/1.1`)
)

type (
	EPOLLHTTPServer struct {
		Handler     RequestHandler
		Server      []byte
		BuzyPolling bool

		nowStr atomic.Value
	}
)

func (s *EPOLLHTTPServer) ListenAndServe(addr string) error {
	cpu := runtime.NumCPU()
	log.Println(`Use`, cpu, `listeners`)

	serverFds := make([]int, 0, cpu)
	epollFds := make([]int, 0, cpu)

	closed := false

	defer func() {
		closed = true

		for i := range serverFds {
			serverFd := serverFds[i]
			epollFd := epollFds[i]

			syscall.Close(epollFd)
			syscall.Close(serverFd)
		}
	}()

	s.nowStr.Store(time.Now().AppendFormat(nil, time.RFC1123))
	go func() {
		var buf []byte
		for !closed {
			buf = time.Now().AppendFormat(buf[:0], time.RFC1123)
			s.nowStr.Store(buf)
			time.Sleep(1 * time.Second)
		}
	}()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	portNum, err := net.LookupPort(`tcp`, port)
	if err != nil {
		return err
	}

	for i := 1; i <= cpu; i++ {
		if serverFd, err := socketCreateListener(portNum); err != nil {
			return err
		} else if epollFd, err := socketCreateListenerEpoll(serverFd); err != nil {
			syscall.Close(serverFd)
			return err
		} else {
			serverFds = append(serverFds, serverFd)
			epollFds = append(epollFds, epollFd)
		}
	}

	var wg sync.WaitGroup

	runLocker := make(chan struct{})

	wg.Add(len(serverFds))
	for i := range serverFds {
		serverFd := serverFds[i]
		epollFd := epollFds[i]

		go func() {
			runtime.LockOSThread()
			runLocker <- struct{}{}

			s.epollLoop(epollFd, serverFd)
		}()
		<-runLocker
	}
	wg.Wait()

	return nil
}

func (s *EPOLLHTTPServer) epollLoop(epollFd, serverFd int) {
	var (
		epollEvent  syscall.EpollEvent
		epollEvents [MaxEpollEvents]syscall.EpollEvent

		activeCtx = make(map[int]*RequestCtx, 10)
		usedCtx   []*RequestCtx

		ctx *RequestCtx
	)

	closeClientFd := func(fd int) {
		if ctx, ok := activeCtx[fd]; !ok {
			return
		} else {
			syscall.Close(fd)
			delete(activeCtx, fd)
			usedCtx = append(usedCtx, ctx)
		}
	}

	epollWaitTimeout := -1
	if s.BuzyPolling {
		epollWaitTimeout = 0
	}

	for {
		nEvents, err := syscall.EpollWait(epollFd, epollEvents[:], epollWaitTimeout)
		if err != nil {
			if errno, ok := err.(syscall.Errno); ok && (errno == syscall.EINTR) {
				continue
			}
			log.Println(`EpollWait: `, err)
			break
		}

		for ev := 0; ev < nEvents; ev++ {
			fd := int(epollEvents[ev].Fd)
			events := epollEvents[ev].Events

			if (events&syscall.EPOLLERR != 0) || (events&syscall.EPOLLHUP != 0) || (events&syscall.EPOLLIN == 0) {
				closeClientFd(fd)
				continue
			} else if fd == serverFd {
				for {
					connFd, _, err := syscall.Accept(serverFd)

					if err != nil {
						if errno, ok := err.(syscall.Errno); ok {
							if errno == syscall.EAGAIN {
								// обработаны все новые коннекты
							} else {
								log.Printf("Accept: errno: %v\n", errno)
							}
						} else {
							log.Printf("Accept: %T %s\n", err, err)
						}
						break
					} else if err := socketSetNonBlocking(connFd); err != nil {
						log.Println("setSocketNonBlocking: ", err)
						break
					}

					epollEvent.Events = syscall.EPOLLIN | EPOLLET // | syscall.EPOLLOUT
					epollEvent.Fd = int32(connFd)
					if err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, connFd, &epollEvent); err != nil {
						log.Println("EpollCtl: ", err)
						break
					}
				}

				continue
			}

			// обработка основых соединений

			var ok bool
			if ctx, ok = activeCtx[fd]; !ok {
				if l := len(usedCtx); l > 0 {
					ctx = usedCtx[l-1]
					usedCtx = usedCtx[:l-1]
				} else {
					ctx = &RequestCtx{}
				}
				ctx.reset()
				activeCtx[fd] = ctx
			}

			for {
				nbytes, err := syscall.Read(fd, ctx.inputBuf[ctx.inputBufOffs:])

				if err != nil {
					allOk := false
					if errno, ok := err.(syscall.Errno); ok {
						if errno == syscall.EAGAIN {
							// обработаны все новые данные
							allOk = true
						} else if errno == syscall.EBADF {
							// видимо, соединение уже закрылось и так чуть раньше по другому условию
						} else {
							log.Printf("Read: unknown errno: %v\n", errno)
						}
					} else {
						log.Printf("Read: unknown error type %T: %s\n", err, err)
					}

					if !allOk {
						closeClientFd(fd)
					}

					break
				}

				if nbytes > 0 {
					status, bufNew := s.parseRequest(ctx.inputBuf[ctx.inputBufOffs:ctx.inputBufOffs+nbytes], ctx)

					switch status {
					case parseRequestStatusOk:
						if s.Handler != nil {
							s.Handler(ctx)
						}
					case parseRequestStatusNeedMore:
						ctx.inputBufOffs += nbytes
						continue
					case parseRequestStatusBadRequest:
						ctx.ResponseStatus = 400
					}

					buf := s.buildResponse(ctx)
					if n, err := syscall.Write(fd, buf); err != nil {
						log.Println(`Write`, err)
					} else if n != len(buf) {
						log.Println(`Write`, n, `!=`, len(buf))
					}

					if l := len(bufNew); l > 0 {
						// яндекс.Танк не умеет в нормальные POST запросы, присылая два лишних байта "\r\n"
						// "вычитываю" их и работаю дальше

						if (ctx.Method == MethodPOST) && (l == 2) && (bufNew[0] == '\r') && (bufNew[1] == '\n') {
							// all ok
						} else {
							// фигня какая-то пришла
							closeClientFd(fd)
							break
						}
					}

					// запрос обработан
					ctx.reset()

				} else {
					// соединение закрылось
					closeClientFd(fd)
				}
			}
		}
	}
}

func (s *EPOLLHTTPServer) buildResponse(ctx *RequestCtx) []byte {
	var line []byte
	switch ctx.ResponseStatus {
	case 200:
		line = line200
	case 400:
		line = line400
	case 404:
		line = line404
	default:
		line = line500
	}

	tmpBuf := ctx.outputBuf

	// формирование ответа
	tmpBuf = append(tmpBuf[:0], line...)

	tmpBuf = append(tmpBuf, "Server: "...)
	tmpBuf = append(tmpBuf, s.Server...)
	tmpBuf = append(tmpBuf, "\r\n"...)

	tmpBuf = append(tmpBuf, "Content-Length: "...)
	tmpBuf = strconv.AppendUint(tmpBuf, uint64(len(ctx.ResponseBody)), 10)
	tmpBuf = append(tmpBuf, "\r\n"...)

	tmpBuf = append(tmpBuf, "Date: "...)
	tmpBuf = append(tmpBuf, s.nowStr.Load().([]byte)...)
	tmpBuf = append(tmpBuf, "\r\n"...)

	if ctx.keepAlive {
		tmpBuf = append(tmpBuf, "Connection: keep-alive\r\n"...)
	}

	tmpBuf = append(tmpBuf, "\r\n"...)

	if ctx.ResponseBody != nil {
		tmpBuf = append(tmpBuf, ctx.ResponseBody...)
	}

	ctx.outputBuf = tmpBuf // на случай расширения буфера

	return tmpBuf
}

func (s *EPOLLHTTPServer) parseRequest(buf []byte, ctx *RequestCtx) (parseRequestStatus, []byte) {
	var idx int

	for {
		switch ctx.state {
		case parseRequestStateBegin:
			if idx = bytes.IndexByte(buf, '\n'); idx == -1 {
				return parseRequestStatusNeedMore, buf
			}

			ctx.state = parseRequestStateHeaders

			// GET /path/to/file HTTP/1.1
			line := buf[:idx-1] // \r\n
			buf = buf[idx+1:]
			if idx = bytes.IndexByte(line, ' '); idx == -1 {
				return parseRequestStatusBadRequest, buf
			}

			// GET
			if method := line[0:idx]; bytes.Equal(method, strGET) {
				ctx.Method = MethodGET
			} else if bytes.Equal(method, strPOST) {
				ctx.Method = MethodPOST
			} else {
				return parseRequestStatusBadRequest, buf
			}
			line = line[idx+1:]

			// /path/to/file
			if idx = bytes.IndexByte(line, ' '); idx == -1 {
				return parseRequestStatusBadRequest, buf
			}
			ctx.Path = line[:idx]

			// HTTP/1.1 поддерживает keep-alive по умолчанию
			if bytes.HasSuffix(line, str11) {
				ctx.keepAlive = true
			}

		case parseRequestStateHeaders:
			if idx = bytes.IndexByte(buf, '\n'); idx == -1 {
				return parseRequestStatusNeedMore, buf
			}

			line := buf[:idx-1] // \r\n
			buf = buf[idx+1:]
			if len(line) == 0 {
				if ctx.contentLength == 0 {
					// все, распарсили запрос
					return parseRequestStatusOk, buf
				} else {
					ctx.state = parseRequestStateBody
				}
			} else if idx = bytes.IndexByte(line, ':'); idx == -1 {
				return parseRequestStatusBadRequest, buf
			} else {
				key, value := line[:idx], bytesTrimLeftInplace(line[idx+1:])
				bytesToLowerInplace(key)

				if bytes.Equal(key, strContentLength) {
					if i64, ok := byteSliceToInt64(value); !ok {
						return parseRequestStatusBadRequest, buf
					} else {
						ctx.contentLength = int(i64)
					}
				} else if bytes.Equal(key, strConnection) {
					bytesToLowerInplace(value)
					if bytes.Equal(value, strKeepAlive) {
						ctx.keepAlive = true
					} else if bytes.Equal(value, strClose) {
						ctx.keepAlive = false
					}
				}
			}

		case parseRequestStateBody:
			l := ctx.contentLength
			if len(buf) < l {
				return parseRequestStatusNeedMore, buf
			}

			ctx.Body = buf[:l]
			buf = buf[l:]

			return parseRequestStatusOk, buf

		default:
			log.Println(`Bug in code: unexpected parse state`)
			return parseRequestStatusBadRequest, buf
		}
	}

	log.Println(`Bug in code: after of HTTPServer.parseRequest loop`)
	return parseRequestStatusBadRequest, buf
}

func socketCreateListener(port int) (serverFd int, err error) {
	addr := syscall.SockaddrInet4{Port: port}
	copy(addr.Addr[:], net.ParseIP(`0.0.0.0`).To4())

	if serverFd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0); err != nil {
		//
	} else if err = socketSetNonBlocking(serverFd); err != nil {
		//
	} else if err = syscall.SetsockoptInt(serverFd, syscall.SOL_SOCKET, SO_REUSEPORT, 1); err != nil {
		//
	} else if err = syscall.Bind(serverFd, &addr); err != nil {
		//
	}

	err = syscall.Listen(serverFd, syscall.SOMAXCONN)

	if err != nil && serverFd != 0 {
		syscall.Close(serverFd)
	}

	return
}

func socketSetNonBlocking(fd int) error {
	return syscall.SetNonblock(fd, true)
}

func socketCreateListenerEpoll(serverFd int) (epollFd int, err error) {
	var event syscall.EpollEvent
	event.Events = syscall.EPOLLIN | EPOLLET
	event.Fd = int32(serverFd)

	epollFd, err = syscall.EpollCreate1(0)
	if err != nil {
		return 0, err
	} else if err = syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, serverFd, &event); err != nil {
		syscall.Close(epollFd)
		return 0, err
	}

	return epollFd, nil
}
