/*
Copyright 2022 The KubeVela Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"fmt"
	"sync"
	"time"

	monitorContext "github.com/kubevela/pkg/monitor/context"
	wfContext "github.com/kubevela/workflow/pkg/context"
	"github.com/kubevela/workflow/pkg/cue/model/value"
	"github.com/kubevela/workflow/pkg/types"
	"github.com/prometheus/common/model"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	// ProviderName is provider name for install.
	ProviderName = "metrics"
)

type provider struct {
	cli client.Client
	ns  string
}

type checkResult struct {
	successTime time.Time
}

var checkRoutine sync.Map

// PromCheck do health check from metrics from prometheus
func (h *provider) PromCheck(ctx monitorContext.Context, wfCtx wfContext.Context, v *value.Value, act types.Action) error {
	stepId, err := v.GetString("stepID")
	if err != nil {
		return err
	}

	addr, err := v.GetString("promAddress")
	c, err := api.NewClient(api.Config{
		Address: addr,
	})
	if err != nil {
		return err
	}
	promCli := v1.NewAPI(c)
	query, err := v.GetString("query")
	if err != nil {
		return err
	}
	resp, _, err := promCli.Query(ctx, query, time.Now())
	if err != nil {
		return err
	}
	conditionStr, err := v.GetString("condition")
	if err != nil {
		return err
	}
	var valueStr string
	switch v := resp.(type) {
	case *model.Scalar:
		valueStr = v.Value.String()
	case model.Vector:
		valueStr = v[0].Value.String()
	default:
		return fmt.Errorf("cannot handle the not scala value")
	}
	template := fmt.Sprintf("if: %s %s", valueStr, conditionStr)
	cueValue, err := value.NewValue(template, nil, "")
	if err != nil {
		return err
	}
	res, err := cueValue.GetBool("if")
	if err != nil {
		return err
	}
	if res {
		d, err := v.GetString("duration")
		fmt.Println(v.String())
		if err != nil {
			return err
		}
		duration, err := time.ParseDuration(d)
		if err != nil {
			return err
		}
		st, err := getSuccessTime(wfCtx, stepId)
		if err != nil {
			err := setSuccessTime(wfCtx, stepId, time.Now().Unix())
			if err != nil {
				return err
			}
			return v.FillObject(false, "result")
		}
		if st == 0 {
			err := setSuccessTime(wfCtx, stepId, time.Now().Unix())
			if err != nil {
				return err
			}
			return v.FillObject(false, "result")
		}
		successTime := time.Unix(st, 0)
		if successTime.Add(duration).Before(time.Now()) {
			return v.FillObject(true, "result")
		}
		return v.FillObject(false, "result")
	}

	err = setSuccessTime(wfCtx, stepId, 0)
	if err != nil {
		return err
	}
	return v.FillObject(false, "result")
}

func setSuccessTime(wfCtx wfContext.Context, stepID string, time int64) error {
	timeValue, err := value.NewValue(fmt.Sprintf(`{time: %d}`, time), nil, "")
	if err != nil {
		return err
	}
	if err = wfCtx.SetVar(timeValue, stepID, "success"); err != nil {
		return err
	}
	return nil
}

func getSuccessTime(wfCtx wfContext.Context, stepID string) (int64, error) {
	t, err := wfCtx.GetVar(stepID, "success", "time")
	if err != nil {
		return 0, err
	}
	return t.GetInt64()
}

// Install register handlers to provider discover.
func Install(p types.Providers) {
	prd := &provider{}
	p.Register(ProviderName, map[string]types.Handler{
		"promCheck": prd.PromCheck,
	})
}
