package search

import (
	"fmt"
	"parquet_search/parser"
	"testing"
)

const filepath = `D:\temp_project\go\parquet_search\parquet_files\File 1`
const col_idx = 12
const searchStringBenchmark = "ProducerStateManager"
const resultant_search_rows = 125

func TestSearchData(t *testing.T) {

	table, err := parser.ParquetParser(filepath)
	if err != nil {
		t.Fatalf("Parsing file error: %v", err)
	}
	defer table.Release()

	res, err := Search_data(&table, col_idx, searchStringBenchmark)
	if err != nil {
		t.Fatalf("Search_data error: %v", err)
	}
	res_len := len(res)
	fmt.Println(res_len)

	if resultant_search_rows != res_len {
		t.Fatalf("Search results count not matching")
	}
}

func BenchmarkSearchData(b *testing.B) {

	table, err := parser.ParquetParser(filepath)
	if err != nil {
		b.Fatalf("Parsing file error: %v", err)
	}
	defer table.Release()

	for n := 0; n < b.N; n++ {
		_, err := Search_data(&table, col_idx, searchStringBenchmark)
		if err != nil {
			b.Fatalf("Search_data error: %v", err)
		}
		// res_len := len(res)
		// fmt.Println(res_len)
	}
}

func BenchmarkGetRows(b *testing.B) {
	table, err := parser.ParquetParser(filepath)
	if err != nil {
		b.Fatalf("Parsing file error: %v", err)
	}
	defer table.Release()

	rows := make([]int, table.NumRows())
	fmt.Println(len(rows))

	for n := 0; n < b.N; n++ {
		_, err := GetRows(&table, rows)
		if err != nil {
			b.Fatalf("GetRows error: %v", err)
		}
	}
}
