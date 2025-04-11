package parser

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

func ParquetParser(filename string) (arrow.Table, error) {

	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		fmt.Println("Error creating Parquet reader:", err)
		return nil, err
	}
	defer pf.Close()

	mem := memory.NewGoAllocator()

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		fmt.Println("Error creating Arrow reader:", err)
		return nil, err
	}

	ctx := context.Background()

	table, err := reader.ReadTable(ctx)
	if err != nil {
		fmt.Println("Error reading table:", err)
		return nil, err
	}

	return table, nil

}
