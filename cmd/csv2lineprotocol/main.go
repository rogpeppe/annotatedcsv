package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rogpeppe/annotatedcsv"
)

// TODO custom field rename/delete

type table struct {
	Columns map[string]column        `json:"columns,omitempty"`
	Rows    []map[string]interface{} `json:"rows"`
}

type column struct {
	Index   int         `json:"index"`
	Group   bool        `json:"group,omitempty"`
	Default interface{} `json:"default,omitempty"`
	Type    string      `json:"type,omitempty"`
}

func main() {
	r := annotatedcsv.NewReader(os.Stdin)
	if err := writeLineProtocol(r, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func writeLineProtocol(r *annotatedcsv.Reader, w io.Writer) error {
	output := bufio.NewWriter(w)
	defer output.Flush()
	for r.NextTable() {
		info, err := tableInfoForColumns(r.Columns())
		if err != nil {
			return fmt.Errorf("cannot get table info for columns: %v", err)
		}
		var line bytes.Buffer
		for r.NextRow() {
			row := r.Row()
			line.Reset()
			line.WriteString(escapeValue(row[info.measurement], measurementEscaper))
			for i, tagName := range info.tagNames {
				line.WriteByte(',')
				// TODO fix tag name quoting
				line.WriteString(escapeValue(tagName, tagNameEscaper))
				line.WriteByte('=')
				// TODO fix tag value quoting
				line.WriteString(escapeValue(row[info.tagIndexes[i]], tagValueEscaper))
			}
			line.WriteByte(' ')
			// TODO fix field name quoting
			line.WriteString(escapeValue(row[info.field], fieldNameEscaper))
			line.WriteByte('=')
			switch v := row[info.value].(type) {
			case int64:
				fmt.Fprintf(&line, "%di", v)
			case uint64:
				fmt.Fprintf(&line, "%du", v)
			case float64:
				fmt.Fprint(&line, v)
			case string:
				// TODO fix string quoting
				fmt.Fprintf(&line, `"%s"`, escapeValue(v, stringFieldEscaper))
			case bool:
				fmt.Fprint(&line, v)
			case time.Time:
				fmt.Fprintf(&line, "%d", v.UnixNano())
			default:
				return fmt.Errorf("unexpected value type in _value %T", v)
			}
			line.WriteByte(' ')
			fmt.Fprintf(&line, "%d\n", row[info.time].(time.Time).UnixNano())
			output.Write(line.Bytes())
		}
	}
	return nil
}

var (
	tagNameEscaper = strings.NewReplacer(
		"\t", `\t`,
		"\n", `\n`,
		"\f", `\f`,
		"\r", `\r`,
		`,`, `\,`,
		` `, `\ `,
		`=`, `\=`,
	)
	tagValueEscaper    = tagNameEscaper
	fieldNameEscaper   = tagNameEscaper
	stringFieldEscaper = strings.NewReplacer(
		`"`, `\"`,
		`\`, `\\`,
	)
	measurementEscaper = strings.NewReplacer(
		"\t", `\t`,
		"\n", `\n`,
		"\f", `\f`,
		"\r", `\r`,
		`,`, `\,`,
		` `, `\ `,
	)
)

func escapeValue(v interface{}, escaper *strings.Replacer) string {
	switch v := v.(type) {
	case int64:
		return fmt.Sprintf("%di", v)
	case uint64:
		return fmt.Sprintf("%du", v)
	case float64:
		return fmt.Sprint(v)
	case bool:
		return fmt.Sprint(v)
	case string:
		return escaper.Replace(v)
	case time.Time:
		return fmt.Sprintf("%di", v.UnixNano())
	default:
		panic(fmt.Errorf("unexpected value type %T", v))
	}
}

type tableInfo struct {
	measurement int
	field       int
	value       int
	time        int
	tagNames    []string
	tagIndexes  []int
}

func tableInfoForColumns(cols []annotatedcsv.Column) (*tableInfo, error) {
	info := tableInfo{
		measurement: -1,
		field:       -1,
		value:       -1,
		time:        -1,
	}
	for i, col := range cols {
		switch col.Name {
		case "_measurement":
			info.measurement = i
			if col.Type != "string" {
				return nil, fmt.Errorf("_measurement column has wrong type, got %q want %q", col.Type, "string")
			}
		case "_field":
			info.field = i
			if col.Type != "string" {
				return nil, fmt.Errorf("_field column has wrong type, got %q want %q", col.Type, "string")
			}
		case "_value":
			info.value = i
		case "_time":
			info.time = i
			if !strings.HasPrefix(col.Type, "dateTime:") {
				return nil, fmt.Errorf("_time column has wrong type, got %q want %q", col.Type, "dateTime:*")
			}
		case "":
			// Ignore.
		default:
			// TODO check for duplicates
			// TODO arbitrary renames.
			// TODO treat some fields as values not tags
			tagName := strings.TrimPrefix(col.Name, "_")
			info.tagNames = append(info.tagNames, tagName)
			info.tagIndexes = append(info.tagIndexes, i)
		}
	}
	if info.measurement == -1 {
		return nil, fmt.Errorf("no _measurement column found in table")
	}
	if info.field == -1 {
		return nil, fmt.Errorf("no _field column found in table")
	}
	if info.value == -1 {
		return nil, fmt.Errorf("no _value column found in table")
	}
	if info.time == -1 {
		return nil, fmt.Errorf("no _time column found in table")
	}
	return &info, nil
}
