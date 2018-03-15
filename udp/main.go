package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	argv struct {
		Help       bool
		Mode       string
		ListenAddr string
		DialAddr   string
		SendPkgs   uint64
		PprofAddr  string
	}
)

const (
	qpsAccumThresh = 20000
)

var (
	payload = bytes.Repeat([]byte(`X`), 32)
)

type MethodSyscall int

const (
	MethodSyscallRecvfrom = MethodSyscall(iota)
	MethodSyscallRead
	MethodSyscallRecvmsg
	MethodSyscallSendto
	MethodSyscallWrite
)

func init() {
	flag.BoolVar(&argv.Help, `help`, false, `Show this help`)
	flag.StringVar(&argv.ListenAddr, `listen`, ``, `Address to listen (:8123)`)
	flag.StringVar(&argv.DialAddr, `dial`, ``, `Address to dial (127.0.0.1:8123)`)
	flag.StringVar(&argv.PprofAddr, `pprof-addr`, ``, `Address to pprof listen (:8124)`)
	flag.StringVar(&argv.Mode, `mode`, `unknown`, `Mode: client - connWrite/syscallSendto/syscallWrite, server - connRead/readFromUDP/readFrom/syscallRecvfrom/syscallRead/syscallRecvmsg`)
	flag.Uint64Var(&argv.SendPkgs, `n`, 1000000, `Number (average) of packages to send in client mode`)
	flag.Parse()
}

var (
	cntTotal uint64
)

func main() {
	if argv.Help {
		flag.PrintDefaults()
		return
	}

	if argv.PprofAddr != `` {
		go func() {
			log.Println(http.ListenAndServe(argv.PprofAddr, nil))
		}()
	}

	startedAt := time.Now()
	go func() {
		prev := uint64(0)
		for range time.Tick(time.Second) {
			cur := atomic.LoadUint64(&cntTotal)
			fmt.Printf("%.3f Mpps / %.3f M pkgs total\n", float64(cur-prev)/1000/1000, float64(cur)/1000/1000)
			prev = cur
		}
	}()

	doneFunc := func() {
		cur := atomic.LoadUint64(&cntTotal)
		diff := time.Now().Sub(startedAt).Nanoseconds() / int64(time.Microsecond)

		fmt.Printf("Total %.3f M pkgs per %d us\n", float64(cur)/1000/1000, diff)
	}

	defer doneFunc()

	go func() {
		ch := make(chan os.Signal, 100)
		signal.Notify(ch, os.Kill, os.Interrupt)
		<-ch

		doneFunc()

		os.Exit(0)
	}()

	runtime.LockOSThread()

	if argv.ListenAddr != `` {
		if false {
			debug.SetGCPercent(20000)
		}

		server()
	} else if argv.DialAddr != `` {
		if false {
			debug.SetGCPercent(20000)
		}

		client()
	} else {
		flag.PrintDefaults()
	}
}

func parseAddr(addrStr string) (syscall.Sockaddr, error) {
	udpAddr, err := net.ResolveUDPAddr(`udp`, addrStr)
	if err != nil {
		return nil, err
	}
	addr := syscall.SockaddrInet4{Port: udpAddr.Port}
	copy(addr.Addr[:], udpAddr.IP.To4())

	return &addr, nil
}

func server() {
	log.Println(`Listen`, argv.ListenAddr)

	switch argv.Mode {
	case `connRead`:
		// net.ResolveUDPAddr + net.ListenUDP + conn.Read
		serverConnRead()

	case `readFromUDP`:
		// net.ResolveUDPAddr + net.ListenUDP + conn.ReadFromUDP
		serverReadFromUDP()

	case `readFrom`:
		// net.ListenPacket + conn.ReadFrom
		serverReadFrom()

	case `syscallRecvfrom`:
		// syscall.Socket + syscall.Bind + syscall.Recvfrom
		serverSyscall(MethodSyscallRecvfrom)

	case `syscallRead`:
		// syscall.Socket + syscall.Bind + syscall.Read
		serverSyscall(MethodSyscallRead)

	case `syscallRecvmsg`:
		// syscall.Socket + syscall.Bind + syscall.Recvmsg
		serverSyscall(MethodSyscallRecvmsg)

	default:
		log.Fatalln(`Wrong server mode:`, argv.Mode)
	}
}

func serverConnRead() {
	udpAddr, err := net.ResolveUDPAddr(`udp`, argv.ListenAddr)
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := net.ListenUDP(`udp`, udpAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	var buf [10 * 1024]byte

	var acc uint64

	for {
		if _, err := conn.Read(buf[:]); err != nil {
			log.Fatalln(err)
		}

		if acc++; acc%qpsAccumThresh == 0 {
			atomic.AddUint64(&cntTotal, acc)
			acc = 0
		}
	}
}

func serverReadFromUDP() {
	udpAddr, err := net.ResolveUDPAddr(`udp`, argv.ListenAddr)
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := net.ListenUDP(`udp`, udpAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	var buf [10 * 1024]byte

	var acc uint64

	for {
		if _, _, err := conn.ReadFromUDP(buf[:]); err != nil {
			log.Fatalln(err)
		}

		if acc++; acc%qpsAccumThresh == 0 {
			atomic.AddUint64(&cntTotal, acc)
			acc = 0
		}
	}
}

func serverReadFrom() {
	conn, err := net.ListenPacket(`udp`, argv.ListenAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	var buf [10 * 1024]byte

	var acc uint64

	for {
		if _, _, err := conn.ReadFrom(buf[:]); err != nil {
			log.Fatalln(err)
		}

		if acc++; acc%qpsAccumThresh == 0 {
			atomic.AddUint64(&cntTotal, acc)
			acc = 0
		}
	}
}

func serverSyscall(method MethodSyscall) {
	addr, err := parseAddr(argv.ListenAddr)
	if err != nil {
		log.Fatalln(err)
	}

	serverFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		log.Fatalln(err)
	}
	defer syscall.Close(serverFd)

	if err := syscall.Bind(serverFd, addr); err != nil {
		log.Fatalln(err)
	}

	var buf [10 * 1024]byte

	var acc uint64

	switch method {
	case MethodSyscallRecvfrom:
		for {
			if _, _, err := syscall.Recvfrom(serverFd, buf[:], 0); err != nil {
				log.Fatalln(err)
			}

			if acc++; acc%qpsAccumThresh == 0 {
				atomic.AddUint64(&cntTotal, acc)
				acc = 0
			}
		}

	case MethodSyscallRead:
		for {
			if _, err := syscall.Read(serverFd, buf[:]); err != nil {
				log.Fatalln(err)
			}

			if acc++; acc%qpsAccumThresh == 0 {
				atomic.AddUint64(&cntTotal, acc)
				acc = 0
			}
		}

	case MethodSyscallRecvmsg:
		for {
			if _, _, _, _, err := syscall.Recvmsg(serverFd, buf[:], nil, 0); err != nil {
				log.Fatalln(err)
			}

			if acc++; acc%qpsAccumThresh == 0 {
				atomic.AddUint64(&cntTotal, acc)
				acc = 0
			}
		}
	}
}

func client() {
	log.Println(`Dial`, argv.DialAddr)

	switch argv.Mode {
	case `connWrite`:
		clientConnWrite()

	case `syscallSendto`:
		clientSyscall(MethodSyscallSendto)

	case `syscallWrite`:
		clientSyscall(MethodSyscallWrite)

	default:
		log.Fatalln(`Wrong client mode:`, argv.Mode)
	}
}

func clientConnWrite() {
	udpAddr, err := net.ResolveUDPAddr(`udp`, argv.DialAddr)
	if err != nil {
		log.Fatalln(err)
	}

	// cli
	conn, err := net.DialUDP(`udp`, nil, udpAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	var acc uint64

	for {
		if _, err := conn.Write(payload); err != nil {
			log.Fatal(err)
		}

		if acc++; acc%qpsAccumThresh == 0 {
			if atomic.AddUint64(&cntTotal, acc) >= argv.SendPkgs {
				return
			}
			acc = 0
		}
	}
}

func clientSyscall(method MethodSyscall) {
	addr, err := parseAddr(argv.DialAddr)
	if err != nil {
		log.Fatalln(err)
	}

	clientFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		log.Fatalln(err)
	}
	defer syscall.Close(clientFd)

	var acc uint64

	switch method {
	case MethodSyscallSendto:
		for {
			err = syscall.Sendto(clientFd, payload, syscall.MSG_DONTWAIT, addr)
			if err != nil {
				log.Fatal(err)
			}

			if acc++; acc%qpsAccumThresh == 0 {
				if atomic.AddUint64(&cntTotal, acc) >= argv.SendPkgs {
					return
				}
				acc = 0
			}
		}

	case MethodSyscallWrite:
		if err := syscall.Connect(clientFd, addr); err != nil {
			log.Fatalln(err)
		}

		for {
			_, err = syscall.Write(clientFd, payload)
			if err != nil {
				log.Fatal(err)
			}

			if acc++; acc%qpsAccumThresh == 0 {
				if atomic.AddUint64(&cntTotal, acc) >= argv.SendPkgs {
					return
				}
				acc = 0
			}
		}
	}
}
