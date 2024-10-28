// utils.go
package fsql

import (
	"fmt"
	"strconv"
	"strings"
)

func IntListToStrComma(list []int) string {
	if len(list) == 0 {
		return ""
	}
	strs := make([]string, len(list))
	for i, v := range list {
		strs[i] = strconv.Itoa(v)
	}
	return strings.Join(strs, ",")
}

func Int64ListToStrComma(list []int64) string {
	if len(list) == 0 {
		return ""
	}
	strs := make([]string, len(list))
	for i, v := range list {
		strs[i] = strconv.FormatInt(v, 10)
	}
	return strings.Join(strs, ",")
}

// Additional utility functions
func StrListToStrComma(list []string) string {
	if len(list) == 0 {
		return ""
	}
	return strings.Join(list, ",")
}

func Placeholders(start, count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return placeholders
}
