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
		want      []float64
		wantPanic bool
	}{
		{
			name: "panics upon bad input",
			args: args{
				min:   -0.01,
				max:   0.00,
				count: 0,
			},
			wantPanic: true,
		},
		{
			name: "creates expected number of default buckets",
			args: args{
				min:   0.01,
				max:   10,
				count: 8,
			},
			want:      []float64{0.01, 0.026826957952797256, 0.07196856730011518, 0.19306977288832497, 0.5179474679231209, 1.3894954943731366, 3.7275937203149367, 9.999999999999991},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				assert.Panics(t, func() {
					generateElasticsearchProcessTimeBuckets(tt.args.min, tt.args.max, tt.args.count)
				}, nil)
				return
			}
			assert.Equalf(t, tt.want, generateElasticsearchProcessTimeBuckets(tt.args.min, tt.args.max, tt.args.count), "generateElasticsearchProcessTimeBuckets(%v, %v, %v)", tt.args.min, tt.args.max, tt.args.count)
		})
	}
}
