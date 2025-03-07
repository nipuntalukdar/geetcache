// Code generated by Thrift Compiler (0.16.0). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	thrift "github.com/apache/thrift/lib/go/thrift"
	thrift83 "thrift"
)

var _ = thrift83.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  Status Put(PutCommand put)")
  fmt.Fprintln(os.Stderr, "  Status ListPut(ListPutCommand listPut)")
  fmt.Fprintln(os.Stderr, "  ListGPResponse ListPop(ListGPCommand lPop)")
  fmt.Fprintln(os.Stderr, "  ListGPResponse ListGet(ListGPCommand lGet)")
  fmt.Fprintln(os.Stderr, "  ListLenResponse ListLen(string key)")
  fmt.Fprintln(os.Stderr, "  GetResponse Get(string key)")
  fmt.Fprintln(os.Stderr, "  DelResponse Delete(string key)")
  fmt.Fprintln(os.Stderr, "  DelResponse DeleteList(string key)")
  fmt.Fprintln(os.Stderr, "  LeaderResponse Leader()")
  fmt.Fprintln(os.Stderr, "  PeersResponse Peers()")
  fmt.Fprintln(os.Stderr, "  Status AddCounter(CAddCommand counter)")
  fmt.Fprintln(os.Stderr, "  Status DeleteCounter(string counterName)")
  fmt.Fprintln(os.Stderr, "  CStatus Increament(CChangeCommand counter)")
  fmt.Fprintln(os.Stderr, "  CStatus Decrement(CChangeCommand counter)")
  fmt.Fprintln(os.Stderr, "  CStatus GetCounterValue(string counterName)")
  fmt.Fprintln(os.Stderr, "  Status CompSwap(CASCommand cas)")
  fmt.Fprintln(os.Stderr, "  HLogStatus HLogCreate(HLogCreateCmd hlcmd)")
  fmt.Fprintln(os.Stderr, "  HLogStatus HLogDelete(string key)")
  fmt.Fprintln(os.Stderr, "  HLogStatus HLogCardinality(string key)")
  fmt.Fprintln(os.Stderr, "  HLogStatus HLogAdd(HLogAddCmd hladd)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  var cfg *thrift.TConfiguration = nil
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), cfg)
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransportConf(trans, cfg)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactoryConf(cfg)
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(cfg)
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryConf(cfg)
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := thrift83.NewGeetcacheServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "Put":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Put requires 1 args")
      flag.Usage()
    }
    arg84 := flag.Arg(1)
    mbTrans85 := thrift.NewTMemoryBufferLen(len(arg84))
    defer mbTrans85.Close()
    _, err86 := mbTrans85.WriteString(arg84)
    if err86 != nil {
      Usage()
      return
    }
    factory87 := thrift.NewTJSONProtocolFactory()
    jsProt88 := factory87.GetProtocol(mbTrans85)
    argvalue0 := thrift83.NewPutCommand()
    err89 := argvalue0.Read(context.Background(), jsProt88)
    if err89 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Put(context.Background(), value0))
    fmt.Print("\n")
    break
  case "ListPut":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ListPut requires 1 args")
      flag.Usage()
    }
    arg90 := flag.Arg(1)
    mbTrans91 := thrift.NewTMemoryBufferLen(len(arg90))
    defer mbTrans91.Close()
    _, err92 := mbTrans91.WriteString(arg90)
    if err92 != nil {
      Usage()
      return
    }
    factory93 := thrift.NewTJSONProtocolFactory()
    jsProt94 := factory93.GetProtocol(mbTrans91)
    argvalue0 := thrift83.NewListPutCommand()
    err95 := argvalue0.Read(context.Background(), jsProt94)
    if err95 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ListPut(context.Background(), value0))
    fmt.Print("\n")
    break
  case "ListPop":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ListPop requires 1 args")
      flag.Usage()
    }
    arg96 := flag.Arg(1)
    mbTrans97 := thrift.NewTMemoryBufferLen(len(arg96))
    defer mbTrans97.Close()
    _, err98 := mbTrans97.WriteString(arg96)
    if err98 != nil {
      Usage()
      return
    }
    factory99 := thrift.NewTJSONProtocolFactory()
    jsProt100 := factory99.GetProtocol(mbTrans97)
    argvalue0 := thrift83.NewListGPCommand()
    err101 := argvalue0.Read(context.Background(), jsProt100)
    if err101 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ListPop(context.Background(), value0))
    fmt.Print("\n")
    break
  case "ListGet":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ListGet requires 1 args")
      flag.Usage()
    }
    arg102 := flag.Arg(1)
    mbTrans103 := thrift.NewTMemoryBufferLen(len(arg102))
    defer mbTrans103.Close()
    _, err104 := mbTrans103.WriteString(arg102)
    if err104 != nil {
      Usage()
      return
    }
    factory105 := thrift.NewTJSONProtocolFactory()
    jsProt106 := factory105.GetProtocol(mbTrans103)
    argvalue0 := thrift83.NewListGPCommand()
    err107 := argvalue0.Read(context.Background(), jsProt106)
    if err107 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ListGet(context.Background(), value0))
    fmt.Print("\n")
    break
  case "ListLen":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ListLen requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.ListLen(context.Background(), value0))
    fmt.Print("\n")
    break
  case "Get":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Get requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.Get(context.Background(), value0))
    fmt.Print("\n")
    break
  case "Delete":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Delete requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.Delete(context.Background(), value0))
    fmt.Print("\n")
    break
  case "DeleteList":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DeleteList requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.DeleteList(context.Background(), value0))
    fmt.Print("\n")
    break
  case "Leader":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "Leader requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.Leader(context.Background()))
    fmt.Print("\n")
    break
  case "Peers":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "Peers requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.Peers(context.Background()))
    fmt.Print("\n")
    break
  case "AddCounter":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AddCounter requires 1 args")
      flag.Usage()
    }
    arg112 := flag.Arg(1)
    mbTrans113 := thrift.NewTMemoryBufferLen(len(arg112))
    defer mbTrans113.Close()
    _, err114 := mbTrans113.WriteString(arg112)
    if err114 != nil {
      Usage()
      return
    }
    factory115 := thrift.NewTJSONProtocolFactory()
    jsProt116 := factory115.GetProtocol(mbTrans113)
    argvalue0 := thrift83.NewCAddCommand()
    err117 := argvalue0.Read(context.Background(), jsProt116)
    if err117 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AddCounter(context.Background(), value0))
    fmt.Print("\n")
    break
  case "DeleteCounter":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DeleteCounter requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.DeleteCounter(context.Background(), value0))
    fmt.Print("\n")
    break
  case "Increament":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Increament requires 1 args")
      flag.Usage()
    }
    arg119 := flag.Arg(1)
    mbTrans120 := thrift.NewTMemoryBufferLen(len(arg119))
    defer mbTrans120.Close()
    _, err121 := mbTrans120.WriteString(arg119)
    if err121 != nil {
      Usage()
      return
    }
    factory122 := thrift.NewTJSONProtocolFactory()
    jsProt123 := factory122.GetProtocol(mbTrans120)
    argvalue0 := thrift83.NewCChangeCommand()
    err124 := argvalue0.Read(context.Background(), jsProt123)
    if err124 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Increament(context.Background(), value0))
    fmt.Print("\n")
    break
  case "Decrement":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Decrement requires 1 args")
      flag.Usage()
    }
    arg125 := flag.Arg(1)
    mbTrans126 := thrift.NewTMemoryBufferLen(len(arg125))
    defer mbTrans126.Close()
    _, err127 := mbTrans126.WriteString(arg125)
    if err127 != nil {
      Usage()
      return
    }
    factory128 := thrift.NewTJSONProtocolFactory()
    jsProt129 := factory128.GetProtocol(mbTrans126)
    argvalue0 := thrift83.NewCChangeCommand()
    err130 := argvalue0.Read(context.Background(), jsProt129)
    if err130 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Decrement(context.Background(), value0))
    fmt.Print("\n")
    break
  case "GetCounterValue":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetCounterValue requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.GetCounterValue(context.Background(), value0))
    fmt.Print("\n")
    break
  case "CompSwap":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CompSwap requires 1 args")
      flag.Usage()
    }
    arg132 := flag.Arg(1)
    mbTrans133 := thrift.NewTMemoryBufferLen(len(arg132))
    defer mbTrans133.Close()
    _, err134 := mbTrans133.WriteString(arg132)
    if err134 != nil {
      Usage()
      return
    }
    factory135 := thrift.NewTJSONProtocolFactory()
    jsProt136 := factory135.GetProtocol(mbTrans133)
    argvalue0 := thrift83.NewCASCommand()
    err137 := argvalue0.Read(context.Background(), jsProt136)
    if err137 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CompSwap(context.Background(), value0))
    fmt.Print("\n")
    break
  case "HLogCreate":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "HLogCreate requires 1 args")
      flag.Usage()
    }
    arg138 := flag.Arg(1)
    mbTrans139 := thrift.NewTMemoryBufferLen(len(arg138))
    defer mbTrans139.Close()
    _, err140 := mbTrans139.WriteString(arg138)
    if err140 != nil {
      Usage()
      return
    }
    factory141 := thrift.NewTJSONProtocolFactory()
    jsProt142 := factory141.GetProtocol(mbTrans139)
    argvalue0 := thrift83.NewHLogCreateCmd()
    err143 := argvalue0.Read(context.Background(), jsProt142)
    if err143 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.HLogCreate(context.Background(), value0))
    fmt.Print("\n")
    break
  case "HLogDelete":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "HLogDelete requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.HLogDelete(context.Background(), value0))
    fmt.Print("\n")
    break
  case "HLogCardinality":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "HLogCardinality requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.HLogCardinality(context.Background(), value0))
    fmt.Print("\n")
    break
  case "HLogAdd":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "HLogAdd requires 1 args")
      flag.Usage()
    }
    arg146 := flag.Arg(1)
    mbTrans147 := thrift.NewTMemoryBufferLen(len(arg146))
    defer mbTrans147.Close()
    _, err148 := mbTrans147.WriteString(arg146)
    if err148 != nil {
      Usage()
      return
    }
    factory149 := thrift.NewTJSONProtocolFactory()
    jsProt150 := factory149.GetProtocol(mbTrans147)
    argvalue0 := thrift83.NewHLogAddCmd()
    err151 := argvalue0.Read(context.Background(), jsProt150)
    if err151 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.HLogAdd(context.Background(), value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
