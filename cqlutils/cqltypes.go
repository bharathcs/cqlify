package cqlutils

import (
	"fmt"
	"strings"
)

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

func (nativeType CqlNativeType) String() string {
	return []string{
		"ascii", "bigint", "blob", "boolean", "counter", "date", "decimal", "double", "duration", "float", "inet",
		"int", "smallint", "text", "time", "timestamp", "timeuuid", "tinyint", "uuid", "varchar", "varint",
	}[nativeType]
}

func GetCqlNativeType(input string) (CqlNativeType, error) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "ascii":
		return TypeAscii, nil
	case "bigint":
		return TypeBigint, nil
	case "blob":
		return TypeBlob, nil
	case "boolean":
		return TypeBoolean, nil
	case "counter":
		return TypeCounter, nil
	case "date":
		return TypeDate, nil
	case "decimal":
		return TypeDecimal, nil
	case "double":
		return TypeDouble, nil
	case "duration":
		return TypeDuration, nil
	case "float":
		return TypeFloat, nil
	case "inet":
		return TypeInet, nil
	case "int":
		return TypeInt, nil
	case "smallint":
		return TypeSmallint, nil
	case "text":
		return TypeText, nil
	case "time":
		return TypeTime, nil
	case "timestamp":
		return TypeTimestamp, nil
	case "timeuuid":
		return TypeTimeuuid, nil
	case "tinyint":
		return TypeTinyint, nil
	case "uuid":
		return TypeUuid, nil
	case "varchar":
		return TypeVarchar, nil
	case "varint":
		return TypeVarint, nil
	default:
		return -1, fmt.Errorf("could not recognise type '%s'", input)
	}
}

type TableStruct struct {
	TableName string
	Columns   []ColumnsStruct
}

func (t TableStruct) String() string {
	var cols []string
	for _, c := range t.Columns {
		cols = append(cols, fmt.Sprint(c))
	}

	return fmt.Sprintf("{%s: %s}", t.TableName, strings.Join(cols, ", "))
}

type ColumnsStruct struct {
	Name string
	Type CqlNativeType
}

func (c ColumnsStruct) String() string {
	return fmt.Sprintf("%s (%s)", c.Name, c.Type)
}
