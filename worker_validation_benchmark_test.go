package ferricstore

import (
	"strconv"
	"testing"
)

func BenchmarkFirstDuplicateString(b *testing.B) {
	for _, size := range []int{8, 4096} {
		values := make([]string, size)
		for index := range values {
			values[index] = "value-" + strconv.Itoa(index)
		}
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if duplicate, found := firstDuplicateString(values); found {
					b.Fatalf("unexpected duplicate %q", duplicate)
				}
			}
		})
	}
}
