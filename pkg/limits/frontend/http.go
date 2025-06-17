package frontend

import (
	"encoding/json"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"

	"github.com/grafana/loki/v3/pkg/limits/proto"
	"github.com/grafana/loki/v3/pkg/util"
)

type httpCheckLimitsRequest struct {
	Tenant  string                  `json:"tenant"`
	Streams []*proto.StreamMetadata `json:"streams"`
}

type httpCheckLimitsResponse struct {
	Results []*proto.CheckLimitsResult `json:"results,omitempty"`
}

// ServeHTTP implements http.Handler.
func (f *Frontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req httpCheckLimitsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON is invalid or does not match expected schema", http.StatusBadRequest)
		return
	}

	if req.Tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}

	ctx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(r.Context(), req.Tenant))
	if err != nil {
		http.Error(w, "failed to inject org ID", http.StatusInternalServerError)
		return
	}

	resp, err := f.CheckLimits(ctx, &proto.CheckLimitsRequest{
		Tenant:  req.Tenant,
		Streams: req.Streams,
	})
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to check request against limits", "err", err)
		http.Error(w, "an unexpected error occurred while checking request against limits limits", http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, httpCheckLimitsResponse{
		Results: resp.Results,
	})
}
