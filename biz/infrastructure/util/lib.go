package util

import (
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
)

func ParsePagination(opts *basic.PaginationOptions) (p *pagination.PaginationOptions) {
	if opts == nil {
		p = &pagination.PaginationOptions{}
	} else {
		p = &pagination.PaginationOptions{
			Limit:     opts.Limit,
			Offset:    opts.Offset,
			Backward:  opts.Backward,
			LastToken: opts.LastToken,
		}
	}
	return
}
