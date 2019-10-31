package util

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// GetCounterVecValue fetches the current value of a prometheus contervec for the passed label values.   This is inherently
// racy and intended for test use only.
func GetCounterVecValue(counterVec *prometheus.CounterVec, lvs ...string) (float64, error) {
	counter, err := counterVec.GetMetricWithLabelValues(lvs...)
	if err != nil {
		return 0, err
	}

	return GetCounterValue(counter)
}

// GetCounterValue fetches the current value of a prometheus counter.   This is inherently racy and intended for test use
// only.
func GetCounterValue(counter prometheus.Counter) (float64, error) {
	dtoMetric := &dto.Metric{}
	err := counter.Write(dtoMetric)
	if err != nil {
		return 0.0, err
	}
	return *dtoMetric.Counter.Value, nil
}

// GetGaugeVecValue fetches the current value of a prometheus gaugevec for the passed label values.   This is inherently
// racy and intended for test use only.
func GetGaugeVecValue(gaugeVec *prometheus.GaugeVec, lvs ...string) (float64, error) {
	gauge, err := gaugeVec.GetMetricWithLabelValues(lvs...)
	if err != nil {
		return 0, err
	}

	return GetGaugeValue(gauge)
}

// GetGaugeValue fetches the current value of a prometheus gauge.   This is inherently racy and intended for test use
// only.
func GetGaugeValue(gauge prometheus.Gauge) (float64, error) {
	dtoMetric := &dto.Metric{}
	err := gauge.Write(dtoMetric)
	if err != nil {
		return 0.0, err
	}
	return *dtoMetric.Gauge.Value, nil
}
