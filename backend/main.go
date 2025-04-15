package main

import (
	"encoding/json"
	"fmt"
	"os"
	"parquet_search/search"
	"path/filepath"
)

func main() {
	var temp map[string][]map[string]string

	parquet_folder := `D:\temp_project\go\parquet_search\parquet_files`
	// json_str := `{"File 1": [{"Message": "ProducerStateManager"}], "File 2": [{"Message": "ProducerStateManager"}]}`
	json_str := `{"File 1": [{"Message": "ProducerStateManager"}]}`

	err := json.Unmarshal([]byte(json_str), &temp)
	if err != nil {
		fmt.Println("Error:", err)
	}

	files, err := os.ReadDir(parquet_folder)
	if err != nil {
		fmt.Printf("Error getting the files from directory = %v", parquet_folder)
	}

	files_list := make([]string, 0)
	for _, entry := range files {
		filepath := filepath.Join(parquet_folder, entry.Name())
		files_list = append(files_list, filepath)
	}

	files_data, err := search.LoadingFiles(files_list)
	if err != nil {
		fmt.Printf("error parsing all the files")
	}

	remove_files := []string{
		"D:\\temp_project\\go\\parquet_search\\parquet_files\\File 14",
		"D:\\temp_project\\go\\parquet_search\\parquet_files\\File 4",
		"D:\\temp_project\\go\\parquet_search\\parquet_files\\File 9",
		"D:\\temp_project\\go\\parquet_search\\parquet_files\\File 7",
	}

	fmt.Println("file data before deleting")
	for file, _ := range files_data {
		fmt.Println("file, val: \t", file)
	}

	search.RemoveFiles(&files_data, remove_files)
	// for _, filepath := range remove_files {
	// 	data := files_data[filepath]
	// 	fmt.Println("before", *data)
	// 	(*data).Filepath = ""
	// 	(*((*data).Table)).Release()
	// 	(*data).Columns = nil
	// 	fmt.Println("after", *data)
	// 	delete(files_data, filepath)
	// }

	fmt.Println("file data after deleting")
	for file, _ := range files_data {
		fmt.Println("file, val: \t", file)
	}

	// res, err := search.QueryData(&files_data, parquet_folder, json_str)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// res = ""

	// fmt.Println(res)

	// // iterating over the file objects
	// for key, val := range temp {
	// 	file, err := search.CreateNewFile(filepath.Join(parquet_folder, key))
	// 	if err != nil {
	// 		fmt.Println("Error:", err)
	// 	}
	// 	// iterating over the list of query objects
	// 	fmt.Println(file.Filepath)
	// 	for _, v := range val {
	// 		// iterating over the query object
	// 		for k1, v1 := range v {
	// 			// fmt.Println("\t", k1, v1)
	// 			col := k1
	// 			search_string := v1

	// 			col_idx := file.Columns[col]
	// 			res_row_idxs, err := search.Search_data(file.Table, col_idx, search_string)
	// 			if err != nil {
	// 				fmt.Println("Error:", err)
	// 			}

	// 			fmt.Println(len(res_row_idxs))

	// 			rows, err := search.GetRows(file.Table, res_row_idxs)
	// 			if err != nil {
	// 				fmt.Println("Error:", err)
	// 			}

	// 			for i, row := range rows {
	// 				fmt.Println(i, row)
	// 			}

	// 			// json_out_data, err := json.Marshal(rows)
	// 			// if err != nil {
	// 			// 	fmt.Println("Error:", err)
	// 			// }

	// 			// fmt.Println(string(json_out_data))

	// 		}
	// 	}
	// }

}
