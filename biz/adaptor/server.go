package adaptor

import (
	"context"
	"github.com/CloudStriver/platform-relation/biz/application/service"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	genrelation "github.com/CloudStriver/service-idl-gen-go/kitex_gen/platform/relation"
)

type RelationServerImpl struct {
	*config.Config
	RelationService service.RelationService
}

func (s *RelationServerImpl) GetRelationPaths(ctx context.Context, req *genrelation.GetRelationPathsReq) (res *genrelation.GetRelationPathsResp, err error) {
	return s.RelationService.GetRelationPaths(ctx, req)
}

func (s *RelationServerImpl) GetRelationCount(ctx context.Context, req *genrelation.GetRelationCountReq) (res *genrelation.GetRelationCountResp, err error) {
	return s.RelationService.GetRelationCount(ctx, req)
}

func (s *RelationServerImpl) GetRelations(ctx context.Context, req *genrelation.GetRelationsReq) (resp *genrelation.GetRelationsResp, err error) {
	return s.RelationService.GetRelations(ctx, req)
}

func (s *RelationServerImpl) DeleteRelation(ctx context.Context, req *genrelation.DeleteRelationReq) (resp *genrelation.DeleteRelationResp, err error) {
	return s.RelationService.DeleteRelation(ctx, req)
}

func (s *RelationServerImpl) CreateRelation(ctx context.Context, req *genrelation.CreateRelationReq) (resp *genrelation.CreateRelationResp, err error) {
	return s.RelationService.CreateRelation(ctx, req)
}

func (s *RelationServerImpl) GetRelation(ctx context.Context, req *genrelation.GetRelationReq) (resp *genrelation.GetRelationResp, err error) {
	return s.RelationService.GetRelation(ctx, req)
}
