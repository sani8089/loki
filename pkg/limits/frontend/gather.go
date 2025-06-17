package frontend

import (
	"context"

	"github.com/grafana/loki/v3/pkg/limits/proto"
)

type checkLimitsGatherer interface {
	// CheckLimits checks if the streams in the request have either reached or
	// exceeded their per-partition limits. It returns more than one response
	// when the requested streams are sharded over two or more limits instances.
	CheckLimits(context.Context, *proto.CheckLimitsRequest) ([]*proto.CheckLimitsResponse, error)
}
