package timeseries

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/RoaringBitmap/roaring"
)

type TimeseriesMetadataAll struct {
	Schema          string
	MetaUIDForward  map[uint64]*MetaStorage
	MetaUIDIndex    map[int]map[string]uint64
	MetaTagKeyIndex map[string]*TimeseriesMeta
}

type TimeseriesMeta struct {
	TagKey          string
	TagValueRoaring map[string]*roaring.Bitmap
}

func (v *TimeseriesMetadataAll) AvailableTagKeys() []string {
	tagKeys := make([]string, 0, len(v.MetaTagKeyIndex))
	for tagKey, _ := range v.MetaTagKeyIndex {
		tagKeys = append(tagKeys, tagKey)
	}
	return tagKeys
}

func (v *TimeseriesMetadataAll) AvailableTagKeysFor(row string) []string {
	tagKeys := make([]string, 0, len(v.MetaTagKeyIndex[row].TagValueRoaring))
	for tagKey, _ := range v.MetaTagKeyIndex[row].TagValueRoaring {
		tagKeys = append(tagKeys, tagKey)
	}
	return tagKeys
}

// return string of METRIC uuid (not meta uuid)
func (v *TimeseriesMetadataAll) QueryOr(column string, filter_or []string) []string {

	metricUIDs := make([]string, 0, 0)

	// just assuming...
	tsColumn := v.MetaTagKeyIndex[column]

	relevantBitmaps := make([]*roaring.Bitmap, 0, len(filter_or))
	for _, filterOr := range filter_or {
		relevantBitmaps = append(relevantBitmaps, tsColumn.TagValueRoaring[filterOr])
	}

	matchingColumns := roaring.ParOr(3, relevantBitmaps...)
	matchingIterator := matchingColumns.Iterator()

	for matchingIterator.HasNext() {

		index := matchingIterator.Next()
		for key, value := range v.MetaUIDIndex[int(index)] {
			metricUIDs = append(metricUIDs, fmt.Sprintf("%s -> %d", key, value))
		}

	}

	return metricUIDs

}

func NewMetadataForGroupType(groupType string) *TimeseriesMetadataAll {

	rawTimeseries, err := os.ReadFile("timeseries/panoptes_timeseries.json")
	if err != nil {
		panic(err)
	}

	var panoptesTimeseries PanoptesTimeseriesSet

	if err := json.Unmarshal(rawTimeseries, &panoptesTimeseries.Series); err != nil {
		panic(err)
	}

	tsMetadataAll := &TimeseriesMetadataAll{
		Schema:          groupType,
		MetaUIDForward:  make(map[uint64]*MetaStorage),
		MetaUIDIndex:    make(map[int]map[string]uint64),
		MetaTagKeyIndex: make(map[string]*TimeseriesMeta),
	}

	for _, panoptesTimeseriesSet := range panoptesTimeseries.Series {
		if panoptesTimeseriesSet.MetricGroupType == groupType {
			timeseriesInfo := panoptesTimeseriesSet.GetUUIDAndTags()
			tsMetadataAll.MetaUIDForward[timeseriesInfo.MetaUID] = timeseriesInfo
		}
	}

	// preprocessing
	//                   tag key    tag value
	tagValues := make(map[string]map[string]int)

	// for each unique meta set
	for _, metaStorageSet := range tsMetadataAll.MetaUIDForward {

		// For each tag.
		for tagKey, tagValue := range metaStorageSet.Dimensions {
			if _, ok := tagValues[tagKey]; !ok {
				tagValues[tagKey] = make(map[string]int)
			}
			tagValues[tagKey][tagValue] = 0

		}

	}

	for tagKey, _ := range tagValues {
		tsMetadataAll.MetaTagKeyIndex[tagKey] = &TimeseriesMeta{
			TagKey:          tagKey,
			TagValueRoaring: make(map[string]*roaring.Bitmap),
		}
	}

	/*
		type TimeseriesMetadataAll struct {
			Schema string
			MetaTagKeyIndex map[string]*TimeseriesMeta
		}
		type TimeseriesMeta struct {
			TagKey string
			TagValueRoaring map[string]*roaring.Bitmap
		}
	*/

	// For each unique set of dimensions
	incrementor := 0
	for _, metaStorageSet := range tsMetadataAll.MetaUIDForward {

		tsMetadataAll.MetaUIDIndex[incrementor] = metaStorageSet.MetricName

		// for each dimension
		for dimensionKey, dimensionValue := range metaStorageSet.Dimensions {

			for tagValueFromSetToIndex, _ := range tagValues[dimensionKey] {

				if _, ok := tsMetadataAll.MetaTagKeyIndex[dimensionKey].TagValueRoaring[tagValueFromSetToIndex]; !ok {
					tsMetadataAll.MetaTagKeyIndex[dimensionKey].TagValueRoaring[tagValueFromSetToIndex] = roaring.NewBitmap()
				}

				if tagValueFromSetToIndex == dimensionValue {
					tsMetadataAll.MetaTagKeyIndex[dimensionKey].TagValueRoaring[tagValueFromSetToIndex].Add(uint32(incrementor))
				}

			}

		}

		incrementor += 1
	}

	return tsMetadataAll

}

// type TSMetaAllTagsForSchemaIndex struct {
// 	DictIndexToUUID map[int]uint64
// }

// type TSMetaTagIndexer struct {
// 	Cardinality     int
// 	TagValueBitmaps map[string]string
// }
