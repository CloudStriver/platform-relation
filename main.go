package main

import (
	"github.com/CloudStriver/go-pkg/utils/kitex/middleware"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/CloudStriver/platform-relation/provider"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/platform/relation/relationservice"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/kitex-contrib/obs-opentelemetry/tracing"
	"net"
)

func main() {
	klog.SetLogger(log.NewKlogLogger())
	s, err := provider.NewRelationServerImpl()
	if err != nil {
		panic(err)
	}
	addr, err := net.ResolveTCPAddr("tcp", s.ListenOn)
	if err != nil {
		panic(err)
	}

	svr := relationservice.NewServer(
		s,
		server.WithServiceAddr(addr),
		server.WithSuite(tracing.NewServerSuite()),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: s.Name}),
		server.WithMiddleware(middleware.LogMiddleware(s.Name)),
	)

	err = svr.Run()
	if err != nil {
		log.Error(err.Error())
	}
}
