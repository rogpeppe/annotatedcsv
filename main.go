package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type table struct {
	Columns map[string]column        `json:"columns,omitempty"`
	Rows    []map[string]interface{} `json:"rows"`
}

type column struct {
	Index   int         `json:"index"`
	Name    string      `json:"name"`
	Group   bool        `json:"group,omitempty"`
	Default interface{} `json:"default,omitempty"`
	Type    string      `json:"type,omitempty"`
}

func main() {
	r := &csvReader{
		r: csv.NewReader(os.Stdin),
	}
	r.r.FieldsPerRecord = -1
	tables, err := readAll(r)
	if err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	data, err := json.MarshalIndent(tables, "", "\t")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot marshal JSON: %v\n", err)
		os.Exit(1)
	}
	os.Stdout.Write(data)
	os.Stdout.Write([]byte{'\n'})
}

func readAll(r *csvReader) ([]*table, error) {
	var tables []*table
	for {
		table, err := readTable(r)
		if err != nil {
			return tables, err
		}
		tables = append(tables, table)
		if _, err := r.Peek(); err != nil {
			if err == io.EOF {
				return tables, nil
			}
			return nil, err
		}
	}
}

func readTable(r *csvReader) (*table, error) {
	cols, err := readHeader(r)
	if err != nil {
		return nil, err
	}
	rows, err := readTableRows(r, cols)
	if err != nil {
		return nil, err
	}
	columnsMap := make(map[string]column)
	for i, col := range cols {
		if col.Name == "" && col.Default == nil {
			continue
		}
		col.Index = i
		columnsMap[col.Name] = col
	}
	return &table{
		Columns: columnsMap,
		Rows:    rows,
	}, nil
}

func readTableRows(r *csvReader, cols []column) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}
	for {
		row, err := r.Peek()
		if err != nil {
			return rows, nil
		}
		if len(row) > 0 && strings.HasPrefix(row[0], "#") {
			// Start of next table.
			return rows, nil
		}
		r.Read()
		if len(row) != len(cols) {
			return rows, fmt.Errorf("inconsistent number of columns at line %d", r.line)
		}
		rowMap := make(map[string]interface{})
		for i, val := range row {
			col := cols[i]
			if col.Default != nil && val == "" {
				rowMap[col.Name] = col.Default
				continue
			}
			if val == "" && col.Name == "" {
				continue
			}
			x, err := convertToType(val, col.Type)
			if err != nil {
				return rows, fmt.Errorf("cannot parse %q as type %q at line %d", val, col.Type, r.line)
			}
			rowMap[col.Name] = x
		}
		rows = append(rows, rowMap)
	}
}

func readHeader(r *csvReader) ([]column, error) {
	var cols []column
	var defaults []string
	for {
		row, err := r.Peek()
		if err != nil {
			return cols, nil
		}
		r.Read()
		if cols == nil {
			if len(row) == 0 {
				return nil, fmt.Errorf("no columns in table header at line %d", r.line)
			}
			cols = make([]column, len(row))
		} else if len(row) != len(cols) {
			return nil, fmt.Errorf("inconsistent table header (got %d items want %d)", len(row), len(cols))
		}
		if !strings.HasPrefix(row[0], "#") {
			for i, col := range row {
				cols[i].Name = col
			}
			break
		}
		switch row[0] {
		case "#datatype":
			for i := 1; i < len(row); i++ {
				cols[i].Type = row[i]
			}
		case "#group":
			for i := 1; i < len(row); i++ {
				// TODO parse bool?
				cols[i].Group = row[i] == "true"
			}
		case "#default":
			defaults = row
		default:
			fmt.Fprintf(os.Stderr, "unknown column annotation %q\n", row[0])
		}
	}
	if defaults != nil {
		for i := 1; i < len(defaults); i++ {
			if defaults[i] == "" {
				continue
			}
			x, err := convertToType(defaults[i], cols[i].Type)
			if err != nil {
				return nil, fmt.Errorf("cannot convert default value %q to type %q: %v", defaults[i], cols[i].Type, err)
			}
			cols[i].Default = x
		}
	}
	return cols, nil
}

func convertToType(s string, typ string) (interface{}, error) {
	switch typ {
	case "boolean":
		return strconv.ParseBool(s)
	case "long":
		return strconv.ParseInt(s, 10, 64)
	case "unsignedLong":
		return strconv.ParseUint(s, 10, 64)
	case "double":
		x, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return s, nil
		}
		return x, nil
	case "string", "tag", "":
		return s, nil
	}
	if timeFormat := strings.TrimPrefix(typ, "dateTime:"); len(timeFormat) != len(typ) {
		layout := timeFormats[timeFormat]
		if layout == "" {
			return nil, fmt.Errorf("unknown time format %q", typ)
		}
		return time.Parse(layout, s)
	}
	fmt.Fprintf(os.Stderr, "unknown datatype %q\n", typ)
	return s, nil
}

var timeFormats = map[string]string{
	"RFC3339":     time.RFC3339,
	"RFC3339Nano": time.RFC3339Nano,
}

type csvReader struct {
	hasPeeked bool
	row       []string
	err       error
	r         *csv.Reader
	line      int
}

func (r *csvReader) Read() ([]string, error) {
	if r.hasPeeked {
		row, err := r.row, r.err
		r.hasPeeked = false
		return row, err
	}
	r.line++
	return r.r.Read()
}

func (r *csvReader) Peek() ([]string, error) {
	if r.hasPeeked {
		return r.row, r.err
	}
	r.line++
	r.hasPeeked = true
	r.row, r.err = r.r.Read()
	return r.row, r.err
}
