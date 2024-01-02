package provider

import (
	"github.com/CloudStriver/platform-relation/biz/application/service"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/mapper/relation"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/stores/redis"
	"github.com/google/wire"
)

var AllProvider = wire.NewSet(
	ApplicationSet,
	InfrastructureSet,
)

var ApplicationSet = wire.NewSet(
	service.RelationSet,
)

var InfrastructureSet = wire.NewSet(
	config.NewConfig,
	redis.NewRedis,
	MapperSet,
)

var MapperSet = wire.NewSet(
	relation.NewMongoMapper,
)
