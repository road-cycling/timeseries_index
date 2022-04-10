package main

import (
	"fmt"

	"github.com/road-cycling/timeseries"
)

func main() {
	fmt.Println("roaring.")

	columnIndex := timeseries.NewMetadataForGroupType("cpu")

	fmt.Println(columnIndex)
	fmt.Println(columnIndex.AvailableTagKeys())
	fmt.Println(columnIndex.AvailableTagKeysFor("cpu_type"))

	fmt.Println(columnIndex.QueryOr("cpu_type", []string{"ctrl"}))
	fmt.Println(columnIndex.QueryOr("cpu_type", []string{"data"}))

	fmt.Println(columnIndex.QueryOr("cpu_type", []string{"ctrl", "data"}))

}
