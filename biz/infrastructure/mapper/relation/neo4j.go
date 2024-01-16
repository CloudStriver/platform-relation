package relation

import (
	"context"
	"fmt"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/CloudStriver/platform-relation/biz/infrastructure/config"
	genrelation "github.com/CloudStriver/service-idl-gen-go/kitex_gen/platform/relation"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/zeromicro/go-zero/core/mr"
	"strconv"
	"time"
)

const EdgeName = "relation"

var _ RelationNeo4jMapper = (*Neo4jMapper)(nil)

type (
	RelationNeo4jMapper interface {
		CreateEdge(ctx context.Context, relation *genrelation.RelationInfo) error
		MatchEdge(ctx context.Context, relation *genrelation.RelationInfo) (bool, error)
		DeleteEdge(ctx context.Context, relation *genrelation.RelationInfo) error
		MatchFromEdges(ctx context.Context, fromType int64, fromId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, error)
		MatchToEdges(ctx context.Context, toType int64, toId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, error)
		MatchFromEdgesCount(ctx context.Context, fromType int64, fromId string, relationType *int64) (int64, error)
		MatchToEdgesCount(ctx context.Context, toType int64, toId string, relationType *int64) (int64, error)
		MatchFromEdgesAndCount(ctx context.Context, fromType int64, fromId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, int64, error)
		MatchToEdgesAndCount(ctx context.Context, toType int64, toId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, int64, error)
	}
	Neo4jMapper struct {
		conn     neo4j.DriverWithContext
		DataBase string
	}
)

func (n Neo4jMapper) MatchFromEdgesAndCount(ctx context.Context, fromType int64, fromId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, int64, error) {
	options.EnsureSafe()
	relation := make([]*genrelation.Relation, 0, *options.Limit)
	var count int64
	var err error

	if err = mr.Finish(func() error {
		relation, err = n.MatchFromEdges(ctx, fromType, fromId, relationType, options)
		if err != nil {
			return err
		}
		return nil
	}, func() error {
		count, err = n.MatchFromEdgesCount(ctx, fromType, fromId, relationType)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}

	return relation, count, nil
}

func (n Neo4jMapper) MatchToEdgesAndCount(ctx context.Context, toType int64, toId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, int64, error) {
	options.EnsureSafe()
	relation := make([]*genrelation.Relation, 0, *options.Limit)
	var count int64
	var err error
	if err = mr.Finish(func() error {
		relation, err = n.MatchToEdges(ctx, toType, toId, relationType, options)
		if err != nil {
			return err
		}
		return nil
	}, func() error {
		count, err = n.MatchToEdgesCount(ctx, toType, toId, relationType)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}

	return relation, count, nil
}

func (n Neo4jMapper) MatchFromEdgesCount(ctx context.Context, fromType int64, fromId string, relationType *int64) (int64, error) {
	r := ""
	if relationType != nil {
		r = ":" + IdToLabel(*relationType)
	}
	query := fmt.Sprintf("MATCH (n1:%s{name: $FromId})-[r%s]->(n2) RETURN COUNT(r)", IdToLabel(fromType), r)
	result, err := neo4j.ExecuteQuery(ctx, n.conn, query, map[string]any{"FromId": fromId},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase))
	if err != nil {
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return 0, err
	}
	count, _, err := neo4j.GetRecordValue[int64](result.Records[0], "COUNT(r)")
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (n Neo4jMapper) MatchToEdgesCount(ctx context.Context, toType int64, toId string, relationType *int64) (int64, error) {
	r := ""
	if relationType != nil {
		r = ":" + IdToLabel(*relationType)
	}
	query := fmt.Sprintf("MATCH (n1)-[r%s]->(n2:%s{name: $ToId}) RETURN COUNT(r)", r, IdToLabel(toType))
	result, err := neo4j.ExecuteQuery(ctx, n.conn, query, map[string]any{"ToId": toId},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase))
	if err != nil {
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return 0, err
	}
	count, _, err := neo4j.GetRecordValue[int64](result.Records[0], "COUNT(r)")
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (n Neo4jMapper) MatchFromEdges(ctx context.Context, fromType int64, fromId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, error) {
	options.EnsureSafe()
	r := ""
	if relationType != nil {
		r = ":" + IdToLabel(*relationType)
	}
	query := fmt.Sprintf("MATCH (n1:%s{name: $FromId})-[r%s]->(n2) RETURN n2,r ORDER BY r.createTime DESC SKIP $Offset LIMIT $Limit", IdToLabel(fromType), r)
	result, err := neo4j.ExecuteQuery(ctx, n.conn, query, map[string]any{"FromId": fromId, "Offset": *options.Offset, "Limit": *options.Limit},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase))
	if err != nil {
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return nil, err
	}

	relation := make([]*genrelation.Relation, 0, len(result.Records))
	for i := range result.Records {
		relationNode, _, err := neo4j.GetRecordValue[neo4j.Relationship](result.Records[i], "r")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		toNode, _, err := neo4j.GetRecordValue[neo4j.Node](result.Records[i], "n2")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		toId, err := neo4j.GetProperty[string](toNode, "name")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		createTime, err := neo4j.GetProperty[int64](relationNode, "createTime")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		relation = append(relation, &genrelation.Relation{
			ToType:       LabelToId(toNode.Labels[0]),
			ToId:         toId,
			FromType:     fromType,
			FromId:       fromId,
			RelationType: LabelToId(relationNode.Type),
			CreateTime:   createTime,
		})
	}
	return relation, nil
}

func (n Neo4jMapper) MatchToEdges(ctx context.Context, toType int64, toId string, relationType *int64, options *pagination.PaginationOptions) ([]*genrelation.Relation, error) {
	r := ""
	if relationType != nil {
		r = ":" + IdToLabel(*relationType)
	}
	query := fmt.Sprintf("MATCH (n1)-[r%s]->(n2:%s{name:$ToId}) RETURN n1,r ORDER BY r.createTime DESC SKIP $Offset LIMIT $Limit", r, IdToLabel(toType))
	result, err := neo4j.ExecuteQuery(ctx, n.conn, query, map[string]any{"ToId": toId, "Offset": *options.Offset, "Limit": *options.Limit},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase))
	if err != nil {
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return nil, err
	}
	relation := make([]*genrelation.Relation, 0, len(result.Records))
	for i := range result.Records {
		relationNode, _, err := neo4j.GetRecordValue[neo4j.Relationship](result.Records[i], "r")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		fromNode, _, err := neo4j.GetRecordValue[neo4j.Node](result.Records[i], "n1")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}
		fromId, err := neo4j.GetProperty[string](fromNode, "name")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		createTime, err := neo4j.GetProperty[int64](relationNode, "createTime")
		if err != nil {
			log.CtxError(ctx, "GetRecordValue异常[%v]\n", err)
			return nil, err
		}

		relation = append(relation, &genrelation.Relation{
			FromType:     LabelToId(fromNode.Labels[0]),
			FromId:       fromId,
			ToType:       toType,
			ToId:         toId,
			RelationType: LabelToId(relationNode.Type),
			CreateTime:   createTime,
		})
	}
	return relation, nil
}

func IdToLabel(id int64) string {
	return fmt.Sprintf("t%d", id)
}

func LabelToId(label string) int64 {
	numberStr := label[1:]
	id, _ := strconv.ParseInt(numberStr, 10, 64)
	return id
}

func (n Neo4jMapper) CreateEdge(ctx context.Context, relation *genrelation.RelationInfo) error {
	if _, err := neo4j.ExecuteQuery(ctx, n.conn,
		fmt.Sprintf("MERGE (n1:%s{name: $FromId}) MERGE (n2:%s{name: $ToId}) CREATE (n1)-[r:%s{createTime:$CreateTime}]->(n2)",
			IdToLabel(relation.FromType), IdToLabel(relation.ToType), IdToLabel(relation.RelationType)),
		map[string]any{
			"FromId":     relation.FromId,
			"ToId":       relation.ToId,
			"CreateTime": time.Now().UnixMilli(),
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase)); err != nil {
		log.CtxError(ctx, "创建关系异常[%v]\n", err)
		return err
	}
	return nil
}

func (n Neo4jMapper) MatchEdge(ctx context.Context, relation *genrelation.RelationInfo) (bool, error) {
	result, err := neo4j.ExecuteQuery(ctx, n.conn,
		fmt.Sprintf("MATCH (n1:%s{name: $FromId})-[r:%s]->(n2:%s{name: $ToId}) return r",
			IdToLabel(relation.FromType), IdToLabel(relation.RelationType), IdToLabel(relation.ToType)),
		map[string]any{
			"FromId": relation.FromId,
			"ToId":   relation.ToId,
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase))
	if err != nil {
		log.CtxError(ctx, "查询关系异常[%v]\n", err)
		return false, err
	}

	return len(result.Records) != 0, nil
}

func (n Neo4jMapper) DeleteEdge(ctx context.Context, relation *genrelation.RelationInfo) error {
	if _, err := neo4j.ExecuteQuery(ctx, n.conn,
		fmt.Sprintf("MATCH (n1:%s{name: $FromId})-[r:%s]->(n2:%s{name: $ToId}) DELETE r",
			IdToLabel(relation.FromType), IdToLabel(relation.RelationType), IdToLabel(relation.ToType)),
		map[string]any{
			"FromId": relation.FromId,
			"ToId":   relation.ToId,
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(n.DataBase)); err != nil {
		log.CtxError(ctx, "创建关系异常[%v]\n", err)
		return err
	}
	return nil
}

func NewNeo4jMapper(config *config.Config) RelationNeo4jMapper {
	conn, err := neo4j.NewDriverWithContext(config.Neo4jConf.Url, neo4j.BasicAuth(config.Neo4jConf.Username, config.Neo4jConf.Password, ""))
	if err != nil {
		panic(err)
	}
	return &Neo4jMapper{
		conn:     conn,
		DataBase: config.Neo4jConf.DataBase,
	}
}
