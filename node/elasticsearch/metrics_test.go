package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_generateElasticsearchProcessTimeBuckets(t *testing.T) {
	type args struct {
		min   float64
		max   float64
		count int
	}
	tests := []struct {
		name      string
		args      args
		wantCount int
	}{
		{
			name: "creates expected number of default buckets",
			args: args{
				min:   0.01,
				max:   10,
				count: 8,
			},
			wantCount: 8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantCount, len(generateElasticsearchProcessTimeBuckets(tt.args.min, tt.args.max, tt.args.count)), "generateElasticsearchProcessTimeBuckets(%v, %v, %v)", tt.args.min, tt.args.max, tt.args.count)
		})
	}
}
