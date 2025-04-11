package main

import (
	"encoding/json"
	"fmt"
	"parquet_search/search"
	"path/filepath"
)

func main() {
	var temp map[string][]map[string]string

	parquet_folder := `D:\temp_project\go\parquet_search\parquet_files`
	json_str := `{"File 1": [{"Message": "ProducerStateManager"}], "File 2": [{"Message": "ProducerStateManager"}]}`

	err := json.Unmarshal([]byte(json_str), &temp)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// iterating over the objects
	for key, val := range temp {
		file, err := search.CreateNewFile(filepath.Join(parquet_folder, key))
		if err != nil {
			fmt.Println("Error:", err)
		}
		// iterating over the list
		for _, v := range val {
			// iterating over the objects
			for k1, v1 := range v {
				// fmt.Println("\t", k1, v1)
				col := k1
				search_string := v1

				col_idx := file.Columns[col]
				res_row_idxs, err := search.Seach_data(file.Table, col_idx, search_string)
				if err != nil {
					fmt.Println("Error:", err)
				}

				fmt.Println(key, len(res_row_idxs))

				// search.GetRows(file.Table, res_row_idxs)
			}
		}
	}

}
