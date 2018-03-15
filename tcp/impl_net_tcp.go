package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type (
	Method             int
	parseRequestStatus int
	parseRequestState  int
)

const (
	MethodGET = Method(iota)
	MethodPOST
)

const (
	parseRequestStatusOk = parseRequestStatus(iota)
	parseRequestStatusBadRequest
	parseRequestStatusNeedMore
)

const (
	parseRequestStateBegin = parseRequestState(iota)
	parseRequestStateHeaders
	parseRequestStateBody
)

var (
	strGET           = []byte(`GET`)
	strPOST          = []byte(`POST`)
	strContentLength = []byte(`content-length`)
	strConnection    = []byte(`connection`)
	strClose         = []byte(`close`)
	strKeepAlive     = []byte(`keep-alive`)

	line200 = []byte("HTTP/1.1 200 OK\r\n")
	line400 = []byte("HTTP/1.1 400 Bad Request\r\n")
	line404 = []byte("HTTP/1.1 404 Not Found\r\n")
	line500 = []byte("HTTP/1.1 500 Internal Server Error\r\n")
)

type (
	NetTCPHTTPServer struct {
		Handler RequestHandler
		Server  []byte
	}

	RequestHandler func(ctx *RequestCtx)

	RequestCtx struct {
		state         parseRequestState
		contentLength int
		keepAlive     bool

		Method Method
		Path   []byte
		Body   []byte

		ResponseStatus int
		ResponseBody   []byte

		inputBuf     []byte
		inputBufOffs int
		outputBuf    []byte

		UserBuf []byte // может использоваться внутри RequestHandler как угодно, сервер его не трогает
	}
)

func (c *RequestCtx) reset() {
	c.state = parseRequestStateBegin
	c.contentLength = 0
	c.keepAlive = false

	c.Method = MethodGET
	c.Path = nil
	c.Body = nil

	c.ResponseStatus = 200
	c.ResponseBody = nil

	c.inputBufOffs = 0
	if cap(c.inputBuf) == 0 {
		c.inputBuf = make([]byte, 16*1024, 16*1024)
	} else {
		c.inputBuf = c.inputBuf[:]
	}

	if cap(c.outputBuf) == 0 {
		c.outputBuf = make([]byte, 0, 16*1024)
	} else {
		c.outputBuf = c.outputBuf[:0]
	}

	if cap(c.UserBuf) == 0 {
		c.UserBuf = make([]byte, 0, 16*1024)
	} else {
		c.UserBuf = c.UserBuf[:0]
	}
}

func (s *NetTCPHTTPServer) ListenAndServe(addr string) error {
	ln, err := net.Listen(`tcp`, addr)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err == nil {
			go s.serveConn(conn)
		} else if netErr, ok := err.(net.Error); !ok {
			return err
		} else if netErr.Temporary() {
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			return netErr
		}
	}
}

func (s *NetTCPHTTPServer) serveConn(conn net.Conn) {
	var ctx RequestCtx
	ctx.reset()

	var rdBuf = make([]byte, 10240) // не готовы обрабатывать запросы больше 10КБ, но доделать не сложно
	var rdBufOffs int

	var wrBuf = make([]byte, 10240) // буфер будет увеличен, если ответ не влезет

	var closed = false
	defer func() { closed = true }()

	var nowStr atomic.Value
	nowStr.Store(time.Now().AppendFormat(nil, time.RFC1123))
	go func() {
		var buf []byte
		for !closed {
			buf = time.Now().AppendFormat(buf[:0], time.RFC1123)
			nowStr.Store(buf)
			time.Sleep(1 * time.Second)
		}
	}()

loop:
	for {
		n, err := conn.Read(rdBuf[rdBufOffs:])
		if err != nil {
			if err == io.EOF {
				return
			}

			errStr := err.Error()
			if !strings.Contains(errStr, `broken pipe`) &&
				!strings.Contains(errStr, `reset by peer`) &&
				!strings.Contains(errStr, `i/o timeout`) {
				log.Println(`read`, err)
			}

			return
		}

		status, bufNew := s.parseRequest(rdBuf[rdBufOffs:rdBufOffs+n], &ctx)

		switch status {
		case parseRequestStatusOk:
			if s.Handler != nil {
				s.Handler(&ctx)
			}
		case parseRequestStatusNeedMore:
			rdBufOffs += n
			goto loop
		case parseRequestStatusBadRequest:
			ctx.ResponseStatus = 400
		}

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

		// формирование ответа
		wrBuf = append(wrBuf[:0], line...)

		wrBuf = append(wrBuf, "Server: "...)
		wrBuf = append(wrBuf, s.Server...)
		wrBuf = append(wrBuf, "\r\n"...)

		wrBuf = append(wrBuf, "Content-Length: "...)
		wrBuf = strconv.AppendUint(wrBuf, uint64(len(ctx.ResponseBody)), 10)
		wrBuf = append(wrBuf, "\r\n"...)

		wrBuf = append(wrBuf, "Date: "...)
		wrBuf = append(wrBuf, nowStr.Load().([]byte)...)
		wrBuf = append(wrBuf, "\r\n"...)

		if ctx.keepAlive {
			wrBuf = append(wrBuf, "Connection: keep-alive\r\n"...)
		}

		wrBuf = append(wrBuf, "\r\n"...)

		if ctx.ResponseBody != nil {
			wrBuf = append(wrBuf, ctx.ResponseBody...)
		}

		_, err = conn.Write(wrBuf)
		if err != nil {
			log.Println(`write error`, err)
			return
		}

		if !ctx.keepAlive {
			return
		}

		// подготовка к следующему запросу
		ctx.reset()

		if len(bufNew) > 0 {
			copy(rdBuf, bufNew)
			rdBufOffs = len(bufNew)
		} else {
			rdBufOffs = 0
		}
	}
}

func (s *NetTCPHTTPServer) parseRequest(buf []byte, ctx *RequestCtx) (parseRequestStatus, []byte) {
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
			if bytes.HasSuffix(line, []byte(`/1.1`)) {
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

func bytesToLowerInplace(buf []byte) {
	for i, ch := range buf {
		if ch >= 'A' && ch <= 'Z' {
			buf[i] += 'a' - 'A'
		}
	}
}

func bytesTrimLeftInplace(buf []byte) []byte {
	i, l := 0, len(buf)
	for ; i < l && buf[i] == ' '; i++ {
	}
	return buf[i:]
}

func byteSliceToInt64(s []byte) (res int64, ok bool) {
	sign := len(s) > 0 && s[0] == '-'
	if sign {
		s = s[1:]
	}

	ok = true

	res = 0
	for _, c := range s {
		if v := int64(c - '0'); v < 0 || v > 9 {
			ok = false
			break
		} else {
			res = res*10 + v
		}
	}

	if sign {
		res = -res
	}

	return
}
