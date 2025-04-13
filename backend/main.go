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

	// iterating over the file objects
	for key, val := range temp {
		file, err := search.CreateNewFile(filepath.Join(parquet_folder, key))
		if err != nil {
			fmt.Println("Error:", err)
		}
		// iterating over the list of query objects
		fmt.Println(file.Filepath)
		for _, v := range val {
			// iterating over the query object
			for k1, v1 := range v {
				// fmt.Println("\t", k1, v1)
				col := k1
				search_string := v1

				col_idx := file.Columns[col]
				res_row_idxs, err := search.Search_data(file.Table, col_idx, search_string)
				if err != nil {
					fmt.Println("Error:", err)
				}

				fmt.Println(len(res_row_idxs))

				rows, err := search.GetRows(file.Table, res_row_idxs)
				if err != nil {
					fmt.Println("Error:", err)
				}

				// fmt.Println(rows)

				json_out_data, err := json.Marshal(rows)
				if err != nil {
					fmt.Println("Error:", err)
				}

				fmt.Println(string(json_out_data))

			}
		}
	}

}
