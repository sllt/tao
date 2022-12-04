// Code generated by taoctl. DO NOT EDIT!
// Source: hi.proto

package server

import (
	"context"

	"github.com/sllt/tao/tools/taoctl/example/rpc/hi/internal/svc"
	"github.com/sllt/tao/tools/taoctl/example/rpc/hi/pb/hi"
)

type EventServer struct {
	svcCtx *svc.ServiceContext
	hi.UnimplementedEventServer
}

func NewEventServer(svcCtx *svc.ServiceContext) *EventServer {
	return &EventServer{
		svcCtx: svcCtx,
	}
}

func (s *EventServer) AskQuestion(ctx context.Context, in *hi.EventReq) (*hi.EventResp, error) {
	l := eventlogic.NewAskQuestionLogic(ctx, s.svcCtx)
	return l.AskQuestion(in)
}
