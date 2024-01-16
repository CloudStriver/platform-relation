package convertor

import (
	relationmapper "github.com/CloudStriver/platform-relation/biz/infrastructure/mapper/relation"
	genrelation "github.com/CloudStriver/service-idl-gen-go/kitex_gen/platform/relation"
	"github.com/samber/lo"
)

//func RelationMapperToRelation(in *relationmapper.Relation) *genrelation.Relation {
//	return &genrelation.Relation{
//		FromType:     in.FromType,
//		FromId:       in.FromId,
//		ToType:       in.ToType,
//		ToId:         in.ToId,
//		RelationType: in.RelationType,
//		CreateTime:   in.CreateAt.UnixMilli(),
//		UpdateTime:   in.UpdateAt.UnixMilli(),
//	}
//}

//func RelationInfoToRelationMapper(req *genrelation.RelationInfo) *relationmapper.Relation {
//	return &relationmapper.Relation{
//		ToType:       req.ToType,
//		ToId:         req.ToId,
//		FromType:     req.FromType,
//		FromId:       req.FromId,
//		RelationType: req.RelationType,
//	}
//}

func RelationInfoToRelationMongoMapperFilterOptions(req *genrelation.RelationInfo) *relationmapper.FilterOptions {
	return &relationmapper.FilterOptions{
		OnlyFromType:     lo.ToPtr(req.FromType),
		OnlyFromId:       lo.ToPtr(req.FromId),
		OnlyToType:       lo.ToPtr(req.ToType),
		OnlyToId:         lo.ToPtr(req.ToId),
		OnlyRelationType: lo.ToPtr(req.RelationType),
	}
}

//func RelationFilterOptionsToRelationMongoMapperFilterOptions(req *genrelation.RelationFilterOptions) *relationmapper.FilterOptions {
//	return &relationmapper.FilterOptions{
//		OnlyFromType:     req.OnlyFromType,
//		OnlyFromId:       req.OnlyFromId,
//		OnlyToType:       req.OnlyToType,
//		OnlyToId:         req.OnlyToId,
//		OnlyRelationType: req.OnlyRelationType,
//	}
//}
