package cqlparser

import (
	"fmt"
	"github.com/bharathcs/cqlify/cqlutils"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
)

var createTableRegexp = regexp.MustCompile(`(?i)create\s+table\s+(?P<TableName>[^\s]+)\s*\((?P<Columns>.*?)(primary\s+key\s*\(.*\))?\).*`)
var primaryKeyRegexp = regexp.MustCompile(`(?i)\s*primary\s+key\s*`)

func ParseTable(r io.Reader) (cqlutils.TableStruct, error) {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return cqlutils.TableStruct{}, err
	}

	createStatement, err := getCreateTableStatements(string(bytes))
	if err != nil {
		return cqlutils.TableStruct{}, err
	}

	table, err := parseCreateTableStatements(createStatement)
	if err != nil {
		return cqlutils.TableStruct{}, err
	}

	return table, nil
}

func getStatements(input string) []string {
	replacer := strings.NewReplacer("\n", "", "\r", "")
	return strings.Split(replacer.Replace(input), ";")
}

func getCreateTableStatements(input string) (string, error) {
	statements := getStatements(input)

	var res []string
	for _, statement := range statements {
		if createTableRegexp.MatchString(statement) {
			res = append(res, statement)
		}
	}

	if len(res) != 1 {
		return "", fmt.Errorf("found %d number of matches: %v", len(res), res)
	}

	return res[0], nil
}

func parseCreateTableStatements(createTableStatement string) (cqlutils.TableStruct, error) {
	match := createTableRegexp.FindStringSubmatch(createTableStatement)

	paramsMap := make(map[string]string)
	for i, name := range createTableRegexp.SubexpNames() {
		if i > 0 && i <= len(match) {
			paramsMap[name] = match[i]
		}
	}

	tableName, ok1 := paramsMap["TableName"]
	rawColumns, ok2 := paramsMap["Columns"]
	if !ok1 || !ok2 {
		return cqlutils.TableStruct{}, fmt.Errorf("could not parse table: (TableName found - %v; Columns found %v;)", ok1, ok2)
	}

	rawColumns = dropPrimaryKey(rawColumns)

	var columns []cqlutils.ColumnsStruct
	for i, rawColumn := range strings.Split(rawColumns, ",") {
		rawColumn = strings.TrimSpace(rawColumn)
		if len(rawColumn) == 0 {
			continue
		}

		var tokens []string
		for _, token := range strings.Split(rawColumn, " ") {
			token = strings.TrimSpace(token)
			if len(token) == 0 {
				continue
			}
			tokens = append(tokens, token)
		}

		if len(tokens) < 2 {
			return cqlutils.TableStruct{}, fmt.Errorf("could not parse raw column %d '%s'", i, rawColumn)
		}

		cqlType, err := cqlutils.GetCqlNativeType(tokens[1])
		if err != nil {
			return cqlutils.TableStruct{}, fmt.Errorf("could not parse raw column %d '%s'", i, rawColumn)
		}

		columns = append(columns, cqlutils.ColumnsStruct{Name: tokens[0], Type: cqlType})
	}
	return cqlutils.TableStruct{TableName: tableName, Columns: columns}, nil
}

func dropPrimaryKey(rawColumns string) string {
	rawColumnsSplit := primaryKeyRegexp.Split(rawColumns, 2)
	if len(rawColumnsSplit) == 1 {
		return rawColumnsSplit[0]
	}

	startsWithBracket := regexp.MustCompile(`^\s*\(`)
	if startsWithBracket.MatchString(rawColumnsSplit[1]) {
		return rawColumnsSplit[0]
	} else {
		return rawColumnsSplit[0] + " " + rawColumnsSplit[1]
	}
}
