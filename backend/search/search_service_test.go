package search_test

import (
	"encoding/json"
	"os"
	"parquet_search/search"
	"path/filepath"
	"reflect"
	"testing"
)

const parquet_folder = `D:\temp_project\go\parquet_search\parquet_files`
const json_query_string = `{"File 1": [{"Message": "ProducerStateManager"}], "File 2": [{"Message": "ProducerStateManager"}]}`
const test_json_file = `D:\temp_project\go\parquet_search\test_json_files\data.json`

func TestLoadingFiles(t *testing.T) {
	files, err := os.ReadDir(parquet_folder)
	if err != nil {
		t.Fatalf("Error getting the files from directory = %v", parquet_folder)
	}

	files_list := make([]string, 0)
	for _, entry := range files {
		filepath := filepath.Join(parquet_folder, entry.Name())
		files_list = append(files_list, filepath)
	}

	files_data, err := search.LoadingFiles(files_list)
	if err != nil {
		t.Fatalf("error parsing all the files")
	}

	for _, file := range files_list {
		// fmt.Printf("file = %v, loaded_file = %v\n", file, (*(files_data[file])).Filepath)
		if file != (*(files_data[file])).Filepath {
			t.Fatalf("Error: file %v != %v", file, (*(files_data[file])).Filepath)
		}
	}
}

func TestQueryData(t *testing.T) {
	files, err := os.ReadDir(parquet_folder)
	if err != nil {
		t.Fatalf("Error getting the files from directory = %v", parquet_folder)
	}

	files_list := make([]string, 0)
	for _, entry := range files {
		filepath := filepath.Join(parquet_folder, entry.Name())
		files_list = append(files_list, filepath)
	}

	files_data, err := search.LoadingFiles(files_list)
	if err != nil {
		t.Fatalf("error parsing all the files")
	}

	result_json_string, err := search.QueryData(&files_data, parquet_folder, json_query_string)
	if err != nil {
		t.Fatalf("error in function QueryData")
	}

	expected_json_string, err := os.ReadFile(test_json_file)
	if err != nil {
		t.Fatalf("error reading expected json data")
	}

	var expected_data, result_data map[string][]map[string]string
	if err = json.Unmarshal([]byte(expected_json_string), &expected_data); err != nil {
		t.Fatalf("Error parsing expected json data")
	}

	if err = json.Unmarshal([]byte(result_json_string), &result_data); err != nil {
		t.Fatalf("Error parsing expected json data")
	}

	if equal := reflect.DeepEqual(expected_data, result_data); !equal {
		t.Fatalf("result data is not equal to expected data")
	}

}
