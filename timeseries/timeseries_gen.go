package timeseries

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/cespare/xxhash"
)

type PanoptesResource struct {
	ResourceSite     string `json:"resource_site"`
	ResourceClass    string `json:"resource_class"`
	ResourceSubclass string `json:"resource_subclass"`
	ResourceType     string `json:"resource_type"`
	ResourceID       string `json:"resource_id"`
	ResourceEndpoint string `json:"resource_endpoint"`
	ResourcePlugin   string `json:"resource_plugin"`
}

type PanoptesMetric struct {
	MetricName  string  `json:"metric_name"`
	MetricValue float64 `json:"metric_value"`
	MetricType  string  `json:"metric_type"`
}

type PanoptesDimension struct {
	DimensionName  string `json:"dimension_name"`
	DimensionValue string `json:"dimension_Value"`
}

type PanoptesTimeseries struct {
	MetricGroupType          string              `json:"metrics_group_type"`
	MetricGroupInterval      int                 `json:"metrics_group_interval"`
	MetricGroupSchemaVersion string              `json:"metrics_group_schema_version"`
	DeviceMonitored          PanoptesResource    `json:"resource"`
	DeviceMetrics            []PanoptesMetric    `json:"metrics"`
	DeviceDimensions         []PanoptesDimension `json:"dimensions"`
}

type PanoptesTimeseriesSet struct {
	Series []PanoptesTimeseries
}

func ProcessTimeseries() PanoptesTimeseriesSet {
	fmt.Println("called process timeseries")

	rawTimeseries, err := os.ReadFile("timeseries/panoptes_timeseries.json")
	if err != nil {
		panic(err)
	}

	var panoptesTimeseries PanoptesTimeseriesSet

	if err := json.Unmarshal(rawTimeseries, &panoptesTimeseries.Series); err != nil {
		panic(err)
	}

	return panoptesTimeseries

}

func GenerateTagString(dimension []PanoptesDimension) string {
	dimensionContainer := make(map[string]string)
	mapKeysSorted := make([]string, 0)
	for _, panoptesDimension := range dimension {
		dimensionContainer[panoptesDimension.DimensionName] = panoptesDimension.DimensionValue
		mapKeysSorted = append(mapKeysSorted, panoptesDimension.DimensionName)
	}

	if len(mapKeysSorted) != len(dimensionContainer) {
		panic("")
	}

	sort.Strings(mapKeysSorted)
	formatString := ""
	for i, mapKey := range mapKeysSorted {
		if i != 0 {
			formatString += " "
		}
		formatString += fmt.Sprintf("%s=%s", mapKey, dimensionContainer[mapKey])
	}
	return formatString
}

func (s *PanoptesTimeseriesSet) AllSchemaNames() map[string]string {
	groupTypes := make(map[string]string)
	for _, timeseries := range s.Series {
		groupTypes[timeseries.MetricGroupType] = ""
	}
	return groupTypes

}

func (s *PanoptesTimeseriesSet) DumpOpenTSDBFormat() {
	for _, timeseries := range s.Series {

		dimensions := GenerateTagString(timeseries.DeviceDimensions)

		for _, metric := range timeseries.DeviceMetrics {
			fmt.Printf("metric=%s schema=%s id=%s %s %f\n", metric.MetricName, timeseries.MetricGroupType, timeseries.DeviceMonitored.ResourceID, dimensions, metric.MetricValue)
		}

	}
}

type Timeseries struct {
	MetricName         string
	Dimensions         map[string]string
	MetricUID, MetaUID uint64
}

type MetaStorage struct {
	Dimensions map[string]string
	MetricName map[string]uint64
	MetaUID    uint64
}

// metricUUID
func (s *PanoptesTimeseries) GetUUIDAndTags() *MetaStorage {

	tags := GenerateTagString(s.DeviceDimensions)
	tagStringWithSchema := fmt.Sprintf("%s %s", s.MetricGroupType, tags)

	hashDigest := xxhash.New()
	hashDigest.Write([]byte(tagStringWithSchema))

	metaUID := hashDigest.Sum64()

	metaStorage := &MetaStorage{
		Dimensions: make(map[string]string, len(s.DeviceDimensions)),
		MetricName: make(map[string]uint64),
		MetaUID:    metaUID,
	}

	for _, dimensionInfo := range s.DeviceDimensions {
		metaStorage.Dimensions[dimensionInfo.DimensionName] = dimensionInfo.DimensionValue
	}

	for _, timeseries := range s.DeviceMetrics {

		metricUIDHash := xxhash.New()
		metricUIDHash.Write([]byte(fmt.Sprintf("%s %s", timeseries.MetricName, tagStringWithSchema)))

		metaStorage.MetricName[timeseries.MetricName] = metricUIDHash.Sum64()

	}

	return metaStorage
}
