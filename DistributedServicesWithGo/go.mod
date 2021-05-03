module utwoo.com/DistributedServicesWithGo

go 1.16

require (
	github.com/casbin/casbin/v2 v2.28.4
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/raft v1.3.0 // indirect
	github.com/hashicorp/raft-boltdb v0.0.0-20210422161416-485fa74b0b01 // indirect
	github.com/hashicorp/serf v0.9.5
	github.com/kr/pretty v0.1.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/travisjeffery/go-dynaport v1.0.0 // indirect
	github.com/tysontate/gommap v0.0.0-20201017170033-6edfc905bae0
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.16.0
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20210423144448-3a41ef94ed2b
	google.golang.org/grpc v1.37.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v1.0.0
