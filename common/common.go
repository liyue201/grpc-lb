package common

import (
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
	if m, ok := addr.Metadata.(*map[string]string); ok {
		w, ok := (*m)[WeightKey]
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}
