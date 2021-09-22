package connection

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
)

func unaryInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	fmt.Println("Unary interceptor:", method, err)
	fmt.Println("Unary Request:", spew.Sdump(req))
	fmt.Println("Unary response:", spew.Sdump(reply))
	return err
}

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	fmt.Println("wrappedStream RecvMsg:", spew.Sdump(m))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) Trailer() metadata.MD {
	trailer := w.ClientStream.Trailer()
	fmt.Println("wrappedStream Trailers:", spew.Sdump(trailer))
	return trailer
}

func (w *wrappedStream) Header() (metadata.MD, error) {
	header, err := w.ClientStream.Header()
	fmt.Println("wrappedStream Header:", spew.Sdump(header))
	fmt.Println("wrappedStream Header err:", err)
	return header, err
}

func (w *wrappedStream) CloseSend() error {
	err := w.ClientStream.CloseSend()
	fmt.Println("wrappedStream CloseSend err:", err)
	return err
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	fmt.Println("wrappedStream SendMsg:", spew.Sdump(m))
	err := w.ClientStream.SendMsg(m)
	fmt.Println("wrappedStream SendMsg err:", err)
	return err
}

func streamInterceptor(ctx context.Context,
	streamSpecification *grpc.StreamDesc,
	clientConnection *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption) (grpc.ClientStream, error) {
	clientStream, err := streamer(ctx, streamSpecification, clientConnection, method, opts...)
	fmt.Println("Stream interceptor: ", method, err)
	return &wrappedStream{clientStream}, err
}
