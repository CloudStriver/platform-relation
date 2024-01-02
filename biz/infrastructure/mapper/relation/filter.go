package relation

import (
	"github.com/CloudStriver/platform-relation/biz/infrastructure/consts"
	"go.mongodb.org/mongo-driver/bson"
)

type FilterOptions struct {
	OnlyFromType     *int32
	OnlyFromId       *string
	OnlyToType       *int32
	OnlyToId         *string
	OnlyRelationType *int32
}

type MongoFilter struct {
	m bson.M
	*FilterOptions
}

func makeMongoFilter(options *FilterOptions) bson.M {
	return (&MongoFilter{
		m:             bson.M{},
		FilterOptions: options,
	}).toBson()
}

func (f *MongoFilter) toBson() bson.M {
	f.CheckOnlyFromType()
	f.CheckOnlyFromId()
	f.CheckOnlyToType()
	f.CheckOnlyToId()
	f.CheckOnlyRelationType()
	return f.m
}

func (f *MongoFilter) CheckOnlyToId() {
	if f.OnlyToId != nil {
		f.m[consts.ToId] = *f.OnlyToId
	}
}

func (f *MongoFilter) CheckOnlyToType() {
	if f.OnlyToType != nil {
		f.m[consts.ToType] = *f.OnlyToType
	}
}

func (f *MongoFilter) CheckOnlyFromId() {
	if f.OnlyFromId != nil {
		f.m[consts.FromId] = *f.OnlyFromId
	}
}

func (f *MongoFilter) CheckOnlyFromType() {
	if f.OnlyFromType != nil {
		f.m[consts.FromType] = *f.OnlyFromType
	}
}

func (f *MongoFilter) CheckOnlyRelationType() {
	if f.OnlyRelationType != nil {
		f.m[consts.RelationType] = *f.OnlyRelationType
	}
}
