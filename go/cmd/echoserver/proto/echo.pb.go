// Code generated manually — minimal hand-written proto stubs for echo service.
// In production, generate with: protoc --go_out=. --go-grpc_out=. echo.proto
package proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- Messages ----

type EchoRequest struct {
	Message string
}

func (e *EchoRequest) Reset()         {}
func (e *EchoRequest) String() string { return e.Message }
func (e *EchoRequest) ProtoMessage()  {}

type EchoResponse struct {
	Message string
	Pod     string
}

func (e *EchoResponse) Reset()         {}
func (e *EchoResponse) String() string { return e.Message }
func (e *EchoResponse) ProtoMessage()  {}

// ---- Service interfaces ----

type EchoServiceServer interface {
	Echo(context.Context, *EchoRequest) (*EchoResponse, error)
	StreamEcho(EchoService_StreamEchoServer) error
	mustEmbedUnimplementedEchoServiceServer()
}

type UnimplementedEchoServiceServer struct{}

func (UnimplementedEchoServiceServer) Echo(_ context.Context, _ *EchoRequest) (*EchoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Echo not implemented")
}
func (UnimplementedEchoServiceServer) StreamEcho(_ EchoService_StreamEchoServer) error {
	return status.Errorf(codes.Unimplemented, "StreamEcho not implemented")
}
func (UnimplementedEchoServiceServer) mustEmbedUnimplementedEchoServiceServer() {}

type EchoService_StreamEchoServer interface {
	Send(*EchoResponse) error
	Recv() (*EchoRequest, error)
	grpc.ServerStream
}

type EchoServiceClient interface {
	Echo(ctx context.Context, in *EchoRequest, opts ...grpc.CallOption) (*EchoResponse, error)
	StreamEcho(ctx context.Context, opts ...grpc.CallOption) (EchoService_StreamEchoClient, error)
}

type EchoService_StreamEchoClient interface {
	Send(*EchoRequest) error
	Recv() (*EchoResponse, error)
	grpc.ClientStream
}

// ---- Registration ----

var _EchoService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "echo.EchoService",
	HandlerType: (*EchoServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Echo",
			Handler:    _EchoService_Echo_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamEcho",
			Handler:       _EchoService_StreamEcho_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}

func RegisterEchoServiceServer(s *grpc.Server, srv EchoServiceServer) {
	s.RegisterService(&_EchoService_serviceDesc, srv)
}

func _EchoService_Echo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EchoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EchoServiceServer).Echo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/echo.EchoService/Echo"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EchoServiceServer).Echo(ctx, req.(*EchoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _EchoService_StreamEcho_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(EchoServiceServer).StreamEcho(&echoServiceStreamEchoServer{stream})
}

type echoServiceStreamEchoServer struct{ grpc.ServerStream }

func (x *echoServiceStreamEchoServer) Send(m *EchoResponse) error {
	return x.ServerStream.SendMsg(m)
}
func (x *echoServiceStreamEchoServer) Recv() (*EchoRequest, error) {
	m := new(EchoRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
