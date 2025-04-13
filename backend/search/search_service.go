package search

import (
	"fmt"
	"sync"
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
