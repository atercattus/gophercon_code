package main

import (
	"flag"
	"github.com/gin-gonic/gin"
	"github.com/labstack/echo"
	"github.com/valyala/fasthttp"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"
)

type stdlibServer struct {
}

var (
	response     = []byte(`Hello world`)
	serverHdr    = []byte(`gophercon_russia`)
	serverHdrStr = string(serverHdr)
)

func (s *stdlibServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set(`Server`, serverHdrStr)
	resp.Write(response)
}

var (
	argv struct {
		Help      bool
		Mode      string
		Addr      string
		PprofAddr string
	}
)

func init() {
	flag.BoolVar(&argv.Help, `help`, false, `Show this help`)
	flag.StringVar(&argv.Addr, `addr`, `:8123`, `Address to listen`)
	flag.StringVar(&argv.PprofAddr, `pprof-addr`, `:8124`, `Address to pprof listen`)
	flag.StringVar(&argv.Mode, `mode`, `stdlib`, `Server mode: stdlib, gin, echo, fasthttp, nettcp, epoll`)
	flag.Parse()
}

func main() {
	if argv.Help {
		flag.PrintDefaults()
		return
	}

	if false {
		debug.SetGCPercent(20000)
	}

	go func() {
		log.Println(http.ListenAndServe(argv.PprofAddr, nil))
	}()

	switch argv.Mode {
	case `gin`:
		log.Println(`Use gin`)

		gin.SetMode(`release`)
		r := gin.New()
		r.GET(`/`, func(c *gin.Context) {
			c.Writer.Header().Set(`Server`, serverHdrStr)
			c.Writer.Write(response)
		})
		log.Fatal(r.Run(argv.Addr))

	case `echo`:
		log.Println(`Use echo`)

		e := echo.New()
		e.Debug = false
		e.HideBanner = true
		e.GET(`/`, func(c echo.Context) error {
			c.Response().Header().Set(`Server`, serverHdrStr)
			return c.Blob(http.StatusOK, echo.MIMETextPlainCharsetUTF8, response)
		})
		log.Fatal(e.Start(argv.Addr))

	case `fasthttp`:
		log.Println(`Use fasthttp`)

		fasthttp.ListenAndServe(argv.Addr, func(ctx *fasthttp.RequestCtx) {
			/*var v []byte
			for i := 1; i <= 100; i++ {
				v = make([]byte, 8)
			}
			if len(v) != 0 {}*/

			ctx.Response.Header.SetServerBytes(serverHdr)
			ctx.Write(response)
		})

	case `nettcp`:
		log.Println(`Use nettcp`)

		s := NetTCPHTTPServer{
			Server: serverHdr,
			Handler: func(ctx *RequestCtx) {
				ctx.ResponseBody = response
			},
		}
		log.Fatal(s.ListenAndServe(argv.Addr))

	case `epoll`:
		log.Println(`Use epoll`)

		s := EPOLLHTTPServer{
			Server: serverHdr,
			Handler: func(ctx *RequestCtx) {
				ctx.ResponseBody = response
			},
		}
		log.Fatal(s.ListenAndServe(argv.Addr))

	default:
		log.Println(`Use stdlib`)

		s := http.Server{
			Addr:    argv.Addr,
			Handler: &stdlibServer{},
		}

		log.Fatal(s.ListenAndServe())
	}
}
