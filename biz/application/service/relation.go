package service

import (
	"context"
	"errors"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/consts"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/convertor"
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
}

var RelationSet = wire.NewSet(
	wire.Struct(new(RelationServiceImpl), "*"),
	wire.Bind(new(RelationService), new(*RelationServiceImpl)),
)

type RelationServiceImpl struct {
	Config        *config.Config
	RelationModel relationmapper.RelationMongoMapper
	Redis         *redis.Redis
}

func (s *RelationServiceImpl) GetRelations(ctx context.Context, req *genrelation.GetRelationsReq) (resp *genrelation.GetRelationsResp, err error) {
	resp = new(genrelation.GetRelationsResp)

	popt := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)

	relation, total, err := s.RelationModel.FindManyAndCount(ctx, convertor.RelationFilterOptionsToRelationMongoMapperFilterOptions(req.FilterOptions), popt, mongop.IdCursorType)
	if err != nil {
		log.CtxError(ctx, "查找关系异常[%v]\n", err)
		return resp, err
	}

	resp.Total = total
	resp.LastToken = *popt.LastToken
	resp.Relations = make([]*genrelation.Relation, 0, len(relation))
	for _, r := range relation {
		resp.Relations = append(resp.Relations, convertor.RelationMapperToRelation(r))
	}
	return resp, nil
}

func (s *RelationServiceImpl) DeleteRelation(ctx context.Context, req *genrelation.DeleteRelationReq) (resp *genrelation.DeleteRelationResp, err error) {
	resp = new(genrelation.DeleteRelationResp)
	if _, err = s.RelationModel.Delete(ctx, req.Id); err != nil {
		log.CtxError(ctx, "关系删除异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *RelationServiceImpl) CreateRelation(ctx context.Context, req *genrelation.CreateRelationReq) (resp *genrelation.CreateRelationResp, err error) {
	resp = new(genrelation.CreateRelationResp)
	relation := convertor.RelationInfoToRelationMapper(req.Relation)
	if req.IsOnly {
		err = s.RelationModel.FindOneAndDelete(ctx, relation)
		switch {
		case errors.Is(err, consts.ErrNotFound):
			break
		case err != nil:
			log.CtxError(ctx, "查找关系异常[%v]\n", err)
			return resp, err
		case err == nil:
			return resp, nil
		}
	}
	if _, err = s.RelationModel.Insert(ctx, relation); err != nil {
		log.CtxError(ctx, "新增关系异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *RelationServiceImpl) GetRelation(ctx context.Context, req *genrelation.GetRelationReq) (resp *genrelation.GetRelationResp, err error) {
	resp = new(genrelation.GetRelationResp)
	_, err = s.RelationModel.FindOne(ctx, convertor.RelationInfoToRelationMongoMapperFilterOptions(req.RelationInfo))
	switch {
	case errors.Is(err, consts.ErrNotFound):
		resp.Ok = false
		return resp, nil
	case err != nil:
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return resp, err
	default:
		return resp, nil

	}
}
