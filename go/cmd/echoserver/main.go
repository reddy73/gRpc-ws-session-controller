// echoserver is a minimal gRPC server used for drain/rollout testing.
// It implements a bidirectional streaming echo so we can observe
// session survivability during pod termination.
package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/grpc-ws-session-k8s/cmd/echoserver/proto"
)

type echoServer struct {
	pb.UnimplementedEchoServiceServer
}

// Echo is a unary RPC — safe to retry
func (s *echoServer) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	log.Printf("[echo] unary pod=%s msg=%s", podName(), req.Message)
	return &pb.EchoResponse{Message: req.Message, Pod: podName()}, nil
}

// StreamEcho is a bidirectional streaming RPC — unsafe to blindly retry
func (s *echoServer) StreamEcho(stream pb.EchoService_StreamEchoServer) error {
	log.Printf("[echo] stream opened pod=%s", podName())
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("[echo] stream recv error: %v", err)
			return err
		}
		log.Printf("[echo] stream msg pod=%s msg=%s", podName(), req.Message)
		if err := stream.Send(&pb.EchoResponse{Message: req.Message, Pod: podName()}); err != nil {
			return err
		}
	}
}

func main() {
	port := envOrDefault("PORT", "50051")
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.ConnectionTimeout(30*time.Second),
	)

	pb.RegisterEchoServiceServer(srv, &echoServer{})
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		log.Printf("[echoserver] listening pod=%s port=%s", podName(), port)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[echoserver] serve error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[echoserver] draining pod=%s", podName())
	srv.GracefulStop()
	log.Printf("[echoserver] stopped pod=%s", podName())
}

func podName() string { return envOrDefault("POD_NAME", "unknown") }

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
