package relation

import (
	"github.com/CloudStriver/platform-relation/biz/infrastructure/consts"
)

type FilterOptions struct {
	OnlyFromType     *int64
	OnlyFromId       *string
	OnlyToType       *int64
	OnlyToId         *string
	OnlyRelationType *int64
}

type Neo4jFilter struct {
	m map[string]any
	*FilterOptions
}

func makeNeo4jFilter(options *FilterOptions) map[string]any {
	return (&Neo4jFilter{
		m:             make(map[string]any, 5),
		FilterOptions: options,
	}).toMap()
}

func (f *Neo4jFilter) toMap() map[string]any {
	f.CheckOnlyFromType()
	f.CheckOnlyFromId()
	f.CheckOnlyToType()
	f.CheckOnlyToId()
	f.CheckOnlyRelationType()
	return f.m
}

func (f *Neo4jFilter) CheckOnlyToId() {
	if f.OnlyToId != nil {
		f.m[consts.ToId] = *f.OnlyToId
	}
}

func (f *Neo4jFilter) CheckOnlyToType() {
	if f.OnlyToType != nil {
		f.m[consts.ToType] = *f.OnlyToType
	}
}

func (f *Neo4jFilter) CheckOnlyFromId() {
	if f.OnlyFromId != nil {
		f.m[consts.FromId] = *f.OnlyFromId
	}
}

func (f *Neo4jFilter) CheckOnlyFromType() {
	if f.OnlyFromType != nil {
		f.m[consts.FromType] = *f.OnlyFromType
	}
}

func (f *Neo4jFilter) CheckOnlyRelationType() {
	if f.OnlyRelationType != nil {
		f.m[consts.RelationType] = *f.OnlyRelationType
	}
}
