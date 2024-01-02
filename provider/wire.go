//go:build wireinject
// +build wireinject

package provider

import (
	"github.com/CloudStriver/platform-relation/biz/adaptor"
	"github.com/google/wire"
)

func NewRelationServerImpl() (*adaptor.RelationServerImpl, error) {
	wire.Build(
		wire.Struct(new(adaptor.RelationServerImpl), "*"),
		AllProvider,
	)
	return nil, nil
}
