package consts

import "google.golang.org/grpc/status"

var (
	ErrNotFound        = status.Error(10001, "数据不存在")
	ErrInValidObjectID = status.Error(10002, "ID格式错误")
)
