package search

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
)

func CreateFile(filepath string, wg *sync.WaitGroup, ch chan<- *File) {
	defer (*wg).Done()
	file, err := CreateNewFile(filepath)
	if err != nil {
		fmt.Println("Error parsing the file:", filepath, "\nError:", err)
		return
	}

	ch <- file
}

func LoadingFiles(files []string) (map[string]*File, error) {
	var wg sync.WaitGroup
	ch := make(chan *File, len(files))

	for _, file := range files {
		wg.Add(1)
		go CreateFile(file, &wg, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	parsed_file_data := make(map[string]*File)
	for file := range ch {
		parsed_file_data[(*file).Filepath] = file
	}

	return parsed_file_data, nil
}

func SearchDataWorker(table *arrow.Table, col_idx int, search_string string, file string, ch chan<- []int, wg *sync.WaitGroup) {
	defer wg.Done()

	rows_indices, err := Search_data(table, col_idx, search_string)
	if err != nil {
		fmt.Printf("Error searching the search_string = %v, in col_idx = %v, in file = %v \nError: %v", search_string, col_idx, file, err)
		return
	}

	ch <- rows_indices
}

func GetRowsWorker(table *arrow.Table, res_rows []int, file string, ch chan<- []map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()

	rows_data, err := GetRows(table, res_rows)
	if err != nil {
		fmt.Printf("Error getting rows from file = %v \nError: %v\n", file, err)
		return
	}

	ch <- rows_data
}

func QueryData(parsed_file_data *(map[string]*File), parquet_folder string, json_query_string string) (string, error) {
	var query map[string][]map[string]string
	// Example json string
	// json_str := `{"File 1": [{"Message": "ProducerStateManager"}], "File 2": [{"Message": "ProducerStateManager"}]}`

	err := json.Unmarshal([]byte(json_query_string), &query)
	if err != nil {
		fmt.Println("Error parsing json:", err)
		return "", err
	}

	final_output := make(map[string][]map[string]string)

	for filename, query_list := range query {
		fullFilePath := filepath.Join(parquet_folder, filename)

		file := (*parsed_file_data)[fullFilePath]
		table_ptr := (*file).Table
		row_indices_list := make([][]int, 0)

		for _, query := range query_list {
			row_idxs_ch := make(chan []int)
			var searchDataWg sync.WaitGroup

			for col, search_str := range query {
				// fmt.Println((*file).Columns)
				col_idx := (*file).Columns[col]
				// fmt.Println(col_idx)

				searchDataWg.Add(1)
				go SearchDataWorker(table_ptr, col_idx, search_str, fullFilePath, row_idxs_ch, &searchDataWg)

			}

			go func() {
				searchDataWg.Wait()
				close(row_idxs_ch)
			}()

			for val := range row_idxs_ch {
				row_indices_list = append(row_indices_list, val)
			}
		}

		// fmt.Println(row_indices_list)
		res_ch := make(chan []map[string]string)
		var getRowsWg sync.WaitGroup
		for _, rows_indices := range row_indices_list {
			getRowsWg.Add(1)
			go GetRowsWorker(table_ptr, rows_indices, fullFilePath, res_ch, &getRowsWg)
		}

		go func() {
			getRowsWg.Wait()
			close(res_ch)
		}()

		for res_rows := range res_ch {
			// fmt.Println(res_rows)
			// for i, row := range res_rows {
			// 	fmt.Println(i, row)
			// }
			final_output[filename] = res_rows
		}
	}

	output_json_string, err := json.Marshal(final_output)
	if err != nil {
		fmt.Println("Error converting output to Json in QueryData func")
		return "", err
	}

	return string(output_json_string), nil
}
