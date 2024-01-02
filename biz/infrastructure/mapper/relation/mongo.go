package relation

import (
	"context"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/consts"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "relation"

var PrefixRelationCacheKey = "cache:relation:"

var _ RelationMongoMapper = (*MongoMapper)(nil)

type (
	RelationMongoMapper interface {
		Insert(ctx context.Context, data *Relation) (string, error)
		FindOneById(ctx context.Context, id string) (*Relation, error)
		Update(ctx context.Context, data *Relation) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Relation, int32, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Relation, error)
		FindOneAndDelete(ctx context.Context, relation *Relation) error
		FindOne(ctx context.Context, fopts *FilterOptions) (*Relation, error)
	}
	Relation struct {
		ID           primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		ToType       int32              `bson:"toType,omitempty" json:"toType,omitempty"`
		ToId         string             `bson:"toId,omitempty" json:"toId,omitempty"`
		FromType     int32              `bson:"fromType,omitempty" json:"fromType,omitempty"`
		FromId       string             `bson:"fromId,omitempty" json:"fromId,omitempty"`
		RelationType int32              `bson:"relationType,omitempty" json:"relationType,omitempty"`
		UpdateAt     time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		CreateAt     time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) RelationMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindOne(ctx context.Context, fopts *FilterOptions) (*Relation, error) {
	filter := makeMongoFilter(fopts)
	var data *Relation
	err := m.conn.FindOneNoCache(ctx, data, filter)
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	case err != nil:
		return nil, err
	default:
		return data, nil
	}
}

func (m *MongoMapper) FindOneAndDelete(ctx context.Context, relation *Relation) error {
	filter := makeMongoFilter(&FilterOptions{
		OnlyFromType:     lo.ToPtr(relation.FromType),
		OnlyFromId:       lo.ToPtr(relation.FromId),
		OnlyToType:       lo.ToPtr(relation.FromType),
		OnlyToId:         lo.ToPtr(relation.FromId),
		OnlyRelationType: lo.ToPtr(relation.RelationType),
	})
	err := m.conn.FindOneAndDeleteNoCache(ctx, &mongo.SingleResult{}, filter)
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return consts.ErrNotFound
	case err != nil:
		return err
	default:
		return nil
	}
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Relation, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := makeMongoFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Relation
	if err = m.conn.Find(ctx, &data, filter, &options.FindOptions{
		Sort:  sort,
		Limit: popts.Limit,
		Skip:  popts.Offset,
	}); err != nil {
		return nil, err
	}

	if *popts.Backward {
		lo.Reverse(data)
	}

	if len(data) > 0 {
		err = p.StoreCursor(ctx, data[0], data[len(data)-1])
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Relation, int32, error) {
	var data []*Relation
	var total int64

	if err := mr.Finish(func() error {
		var err error
		data, err = m.FindMany(ctx, fopts, popts, sorter)
		return err
	}, func() error {
		var err error
		total, err = m.conn.CountDocuments(ctx, makeMongoFilter(fopts))
		return err
	}); err != nil {
		return nil, 0, err
	}
	return data, int32(total), nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *Relation) (string, error) {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := PrefixRelationCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		return "", err
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), err
}

func (m *MongoMapper) FindOneById(ctx context.Context, id string) (*Relation, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInValidObjectID
	}
	var data Relation
	key := PrefixRelationCacheKey + id
	err = m.conn.FindOne(ctx, key, &data, bson.M{"_id": oid})
	switch err {
	case nil:
		return &data, nil
	default:
		return nil, err
	}
}

func (m *MongoMapper) Update(ctx context.Context, data *Relation) (*mongo.UpdateResult, error) {
	data.UpdateAt = time.Now()
	key := PrefixRelationCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{"_id": data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return 0, err
	}
	key := PrefixRelationCacheKey + id
	res, err := m.conn.DeleteOne(ctx, key, bson.M{"_id": oid})
	return res, err
}
