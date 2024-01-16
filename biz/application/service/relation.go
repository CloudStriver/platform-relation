package service

import (
	"context"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	relationmapper "github.com/CloudStriver/platform-relation/biz/infrastructure/mapper/relation"
	genrelation "github.com/CloudStriver/service-idl-gen-go/kitex_gen/platform/relation"
	"github.com/google/wire"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type RelationService interface {
	CreateRelation(ctx context.Context, req *genrelation.CreateRelationReq) (resp *genrelation.CreateRelationResp, err error)
	GetRelation(ctx context.Context, req *genrelation.GetRelationReq) (resp *genrelation.GetRelationResp, err error)
	DeleteRelation(ctx context.Context, req *genrelation.DeleteRelationReq) (resp *genrelation.DeleteRelationResp, err error)
	GetRelations(ctx context.Context, req *genrelation.GetRelationsReq) (resp *genrelation.GetRelationsResp, err error)
	GetRelationCount(ctx context.Context, req *genrelation.GetRelationCountReq) (resp *genrelation.GetRelationCountResp, err error)
}

var RelationSet = wire.NewSet(
	wire.Struct(new(RelationServiceImpl), "*"),
	wire.Bind(new(RelationService), new(*RelationServiceImpl)),
)

type RelationServiceImpl struct {
	Config        *config.Config
	Redis         *redis.Redis
	RelationModel relationmapper.RelationNeo4jMapper
}

func (s *RelationServiceImpl) GetRelationCount(ctx context.Context, req *genrelation.GetRelationCountReq) (resp *genrelation.GetRelationCountResp, err error) {
	resp = new(genrelation.GetRelationCountResp)
	switch o := req.RelationFilterOptions.(type) {
	case *genrelation.GetRelationCountReq_FromFilterOptions:
		resp.Total, err = s.RelationModel.MatchFromEdgesCount(ctx, o.FromFilterOptions.GetOnlyFromType(), o.FromFilterOptions.GetOnlyFromId(), req.OnlyRelationType)
	case *genrelation.GetRelationCountReq_ToFilterOptions:
		resp.Total, err = s.RelationModel.MatchToEdgesCount(ctx, o.ToFilterOptions.GetOnlyToType(), o.ToFilterOptions.GetOnlyToId(), req.OnlyRelationType)
	}
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RelationServiceImpl) GetRelations(ctx context.Context, req *genrelation.GetRelationsReq) (resp *genrelation.GetRelationsResp, err error) {
	resp = new(genrelation.GetRelationsResp)
	switch o := req.RelationFilterOptions.(type) {
	case *genrelation.GetRelationsReq_FromFilterOptions:
		resp.Relations, resp.Total, err = s.RelationModel.MatchFromEdgesAndCount(ctx, o.FromFilterOptions.OnlyFromType, o.FromFilterOptions.OnlyFromId, req.OnlyRelationType, pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions))
	case *genrelation.GetRelationsReq_ToFilterOptions:
		resp.Relations, resp.Total, err = s.RelationModel.MatchToEdgesAndCount(ctx, o.ToFilterOptions.OnlyToType, o.ToFilterOptions.OnlyToId, req.OnlyRelationType, pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions))
	}
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RelationServiceImpl) DeleteRelation(ctx context.Context, req *genrelation.DeleteRelationReq) (resp *genrelation.DeleteRelationResp, err error) {
	resp = new(genrelation.DeleteRelationResp)
	if err = s.RelationModel.DeleteEdge(ctx, req.RelationInfo); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RelationServiceImpl) CreateRelation(ctx context.Context, req *genrelation.CreateRelationReq) (resp *genrelation.CreateRelationResp, err error) {
	resp = new(genrelation.CreateRelationResp)
	ok, err := s.RelationModel.MatchEdge(ctx, req.RelationInfo)
	if err != nil {
		return resp, err
	}
	if !ok {
		if err = s.RelationModel.CreateEdge(ctx, req.RelationInfo); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (s *RelationServiceImpl) GetRelation(ctx context.Context, req *genrelation.GetRelationReq) (resp *genrelation.GetRelationResp, err error) {
	resp = new(genrelation.GetRelationResp)
	if resp.Ok, err = s.RelationModel.MatchEdge(ctx, req.RelationInfo); err != nil {
		return resp, err
	}
	return resp, nil
}
