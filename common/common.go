package common

import (
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"strconv"
)

const (
	WeightKey = "weight"
)

func GetWeight(addr resolver.Address) int {
	if addr.Metadata == nil {
		return 1
	}
	md, ok := addr.Metadata.(*metadata.MD)
	if ok {
		values := md.Get(WeightKey)
		if len(values) > 0 {
			weight, err := strconv.Atoi(values[0])
			if err == nil {
				return weight
			}
		}
	}
	return 1
}
