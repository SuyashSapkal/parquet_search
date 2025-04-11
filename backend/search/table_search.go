package search

import (
	"fmt"
	"parquet_search/parser"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type File struct {
	Filename string
	Table    *arrow.Table
	Columns  map[string]int
}

func CreateNewFile(filename string) (*File, error) {
	table, err := parser.ParquetParser(filename)
	if err != nil {
		return nil, err
	}
	cols := make(map[string]int, 0)

	schema := table.Schema()

	for i, col := range schema.Fields() {
		cols[col.Name] = i
	}

	file := File{
		Filename: filename,
		Table:    &table,
		Columns:  cols,
	}
	return &file, nil
}

func Seach_data(table *arrow.Table, col_idx int, search_string string) ([]int, error) {
	res_rows := make([]int, 0)

	rows_len := (*table).NumRows()
	for i := 0; i < int(rows_len); i++ {
		col_data := (*table).Column(col_idx).Data().Chunks()
		// fmt.Printf("\n%v", i)
		for _, data := range col_data {
			str_data := ValueToString(&data, i)
			if strings.Contains(str_data, search_string) {
				// fmt.Println(str_data)
				res_rows = append(res_rows, i)
			}
		}
	}
	return res_rows, nil
}

func GetRows(table *arrow.Table, res_rows []int) ([][]string, error) {
	cols_len := (*table).NumCols()
	var rows [][]string
	for i := 0; i < len(res_rows); i++ {
		var row []string
		for j := 0; j < int(cols_len); j++ {
			col_data := (*table).Column(j).Data().Chunks()
			for _, data := range col_data {
				str_data := ValueToString(&data, i)
				row = append(row, str_data)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func ValueToString(arr *arrow.Array, row_index int) string {
	if (*arr).IsNull(row_index) {
		return ""
	}

	switch v := (*arr).(type) {
	case *array.String:
		return v.Value(row_index)
	case *array.Int64:
		return fmt.Sprintf("%d", v.Value(row_index))
	case *array.Float64:
		return fmt.Sprintf("%f", v.Value(row_index))
	case *array.Boolean:
		return fmt.Sprintf("%t", v.Value(row_index))
	default:
		return fmt.Sprintf("%v", (*arr).ValueStr(row_index))
	}
}
