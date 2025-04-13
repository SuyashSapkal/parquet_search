package search_test

import (
	"os"
	"parquet_search/search"
	"path/filepath"
	"testing"
)

const parquet_folder = `D:\temp_project\go\parquet_search\parquet_files`

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
