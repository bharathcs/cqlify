package cqlutils

import (
	"fmt"
	"math"
	"strings"
)

// CqlNativeType (ref: https://docs.scylladb.com/getting-started/types/)
type CqlNativeType int

const (
	TypeAscii CqlNativeType = iota
	TypeBigint
	TypeBlob
	TypeBoolean
	TypeCounter
	TypeDate
	TypeDecimal
	TypeDouble
	TypeDuration
	TypeFloat
	TypeInet
	TypeInt
	TypeSmallint
	TypeText
	TypeTime
	TypeTimestamp
	TypeTimeuuid
	TypeTinyint
	TypeUuid
	TypeVarchar
	TypeVarint
)

var mapping = map[string]CqlNativeType{
	"ascii":     TypeAscii,
	"bigint":    TypeBigint,
	"blob":      TypeBlob,
	"boolean":   TypeBoolean,
	"counter":   TypeCounter,
	"date":      TypeDate,
	"decimal":   TypeDecimal,
	"double":    TypeDouble,
	"duration":  TypeDuration,
	"float":     TypeFloat,
	"inet":      TypeInet,
	"int":       TypeInt,
	"smallint":  TypeSmallint,
	"text":      TypeText,
	"time":      TypeTime,
	"timestamp": TypeTimestamp,
	"timeuuid":  TypeTimeuuid,
	"tinyint":   TypeTinyint,
	"uuid":      TypeUuid,
	"varchar":   TypeVarchar,
	"varint":    TypeVarint,
}

func (nativeType CqlNativeType) String() string {
	arr := []string{
		"ascii", "bigint", "blob", "boolean", "counter", "date", "decimal", "double", "duration", "float",
		"inet", "int", "smallint", "text", "time", "timestamp", "timeuuid", "tinyint", "uuid", "varchar",
		"varint",
	}
	if nativeType < 0 || int(nativeType) >= len(arr) {
		return "INVALID CQL TYPE"
	}
	return arr[nativeType]
}

// GetCqlNativeType expects a string input referencing a native CQL type
func GetCqlNativeType(input string) (CqlNativeType, error) {
	formatted := strings.ToLower(strings.TrimSpace(input))
	if res, ok := mapping[formatted]; ok {
		return res, nil
	}
	return -1, fmt.Errorf("could not recognise type '%s'", input)
}

// TableStruct Representation of a typical table (name & columns)
type TableStruct struct {
	TableName string
	Columns   []ColumnsStruct
}

func (t TableStruct) String() string {
	var cols []string
	for _, c := range t.Columns {
		cols = append(cols, fmt.Sprint(c))
	}

	return fmt.Sprintf("%s: {%s}", t.TableName, strings.Join(cols, ", "))
}

// PrettyString returns a better formatted (multiline, indented) string output for the table.
func (t TableStruct) PrettyString() string {
	if len(t.Columns) == 0 {
		return fmt.Sprintf("%s: {}", t.TableName)
	}

	maxNameLength := 0
	for _, c := range t.Columns {
		maxNameLength = int(math.Max(float64(maxNameLength), float64(len(c.Name))))
	}

	var res []string
	for _, c := range t.Columns {
		var spacing string
		for i := len(c.Name); i <= maxNameLength+1; i++ {
			spacing = spacing + " "
		}
		res = append(res, fmt.Sprint(c.Name, spacing, fmt.Sprint(c.Type)))
	}

	return fmt.Sprintf("%s: {\n%s\n}", t.TableName, strings.Join(res, ",\n"))
}

type ColumnsStruct struct {
	Name string
	Type CqlNativeType
}

func (c ColumnsStruct) String() string {
	return fmt.Sprintf("%s (%s)", c.Name, c.Type)
}
