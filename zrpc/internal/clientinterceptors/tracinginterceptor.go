package clientinterceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"manlu.org/tao/core/trace"
)

// UnaryTracingInterceptor is an interceptor that handles tracing.
func UnaryTracingInterceptor(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, span := trace.StartClientSpan(ctx, cc.Target(), method)
	defer span.Finish()

	var pairs []string
	span.Visit(func(key, val string) bool {
		pairs = append(pairs, key, val)
		return true
	})
	ctx = metadata.AppendToOutgoingContext(ctx, pairs...)

	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamTracingInterceptor is an interceptor that handles tracing for stream requests.
func StreamTracingInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, span := trace.StartClientSpan(ctx, cc.Target(), method)
	defer span.Finish()

	var pairs []string
	span.Visit(func(key, val string) bool {
		pairs = append(pairs, key, val)
		return true
	})
	ctx = metadata.AppendToOutgoingContext(ctx, pairs...)

	return streamer(ctx, desc, cc, method, opts...)
}
