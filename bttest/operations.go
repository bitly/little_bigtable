package bttest

import (
	"context"
	"sync"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type operationsServer struct {
	longrunningpb.UnimplementedOperationsServer
	operations map[string]*longrunningpb.Operation
	mu         sync.RWMutex
}

func (s *operationsServer) GetOperation(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.Name)
	}
	return op, nil
}
