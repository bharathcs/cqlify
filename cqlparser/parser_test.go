package cqlparser

import (
	"fmt"
	"github.com/bharathcs/cqlify/cqlutils"
	"io"
	"reflect"
	"strings"
	"testing"
)

func Test_getCreateTableStatements_checkValidSyntaxPasses(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		wantErr bool
	}{
		{
			args: `
 CREATE TABLE monkeySpecies (
     species text PRIMARY KEY,
     common_name text,
     population varint,
     average_size int
 ) WITH comment='Important biological records'
    AND read_repair_chance = 1.0;`,
			wantErr: false,
		},
		{
			args: `

ignored statements here ;

 CREATE   TABLE  timeline   (
     userid uuid,
     posted_month int,
     posted_time uuid,
     body text,
     posted_by text,
     PRIMARY   KEY (userid, posted_month, posted_time)
 ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

additional ignored statements here ;
ignored statements here again ;
`,

			wantErr: false,
		},
		{
			args: `
 create table loads(
     machine inet,
     cpu int,
     mtime timeuuid,
     load float,
     primaRY Key  ( ( machine, cpu), mtime )
 ) WITH CLUSTERING ORDER BY (mtime DESC);`,
			wantErr: false,
		},
		{
			args: `
  CREATE table users_picture (
     userid uuid,
     pictureid uuid,
     body text,
     posted_by text,
     PRIMARY KEY(userid, pictureid, posted_by)
 ) WITH compression = {'sstable_compression': 'LZ4Compressor'};
`,
			wantErr: false,
		},
		{
			args: `
	
 CreATE TABLE data_atrest (
     pk text PRIMARY KEY,
     c0 int
 ) WITH scylla_encryption_options = {
    'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
    'secret_key_strength' : 128,
    'key_provider': 'LocalFileSystemKeyProviderFactory',
    'secret_key_file': '/etc/scylla/data_encryption_keys/secret_key'};
`,
			wantErr: false,
		},
		{
			args:    `CREATE Table caching (k int PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`,
			wantErr: false,
		},
		{
			name: "No valid statement",
			args: `
cREATE KEYSPACE foo ; 
garbage goes here ;`,
			wantErr: true,
		},
		{
			name:    "Table without name",
			args:    `create table ();`,
			wantErr: true,
		},
		{
			name:    "table name with spaces ",
			args:    `create table bad name ();`,
			wantErr: true,
		},
		{
			name:    "table name with spaces ",
			args:    `create table bad name ();`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := getCreateTableStatements(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCreateTableStatements() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			//if got != tt.want {
			//	t.Errorf("getCreateTableStatements() got = %v, want %v", got, tt.want)
			//}
		})
	}
}

func Test_parseCreateTableStatements(t *testing.T) {
	type args struct {
		createTableStatement string
	}
	tests := []struct {
		name    string
		args    string
		want    cqlutils.TableStruct
		wantErr bool
	}{
		{
			args:    `CREATE TABLE monkeySpecies (species text PRIMARY KEY,common_name text,population varint,average_size int) WITH comment='Important biological records'AND read_repair_chance = 1.0;`,
			want:    cqlutils.TableStruct{TableName: "monkeySpecies", Columns: []cqlutils.ColumnsStruct{{"species", cqlutils.TypeText}, {"common_name", cqlutils.TypeText}, {"population", cqlutils.TypeVarint}, {"average_size", cqlutils.TypeInt}}},
			wantErr: false,
		},
		{
			args:    `CREATE   TABLE  timeline   (userid uuid,posted_month int,posted_time uuid,body text,posted_by text,PRIMARY   KEY (userid, posted_month, posted_time) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };`,
			want:    cqlutils.TableStruct{TableName: "timeline", Columns: []cqlutils.ColumnsStruct{{"userid", cqlutils.TypeUuid}, {"posted_month", cqlutils.TypeInt}, {"posted_time", cqlutils.TypeUuid}, {"body", cqlutils.TypeText}, {"posted_by", cqlutils.TypeText}}},
			wantErr: false,
		},
		{
			args:    `create table loads(machine inet,cpu int,mtime timeuuid,load float,primaRY Key  ( ( machine, cpu), mtime )) WITH CLUSTERING ORDER BY (mtime DESC);`,
			want:    cqlutils.TableStruct{"loads", []cqlutils.ColumnsStruct{{"machine", cqlutils.TypeInet}, {"cpu", cqlutils.TypeInt}, {"mtime", cqlutils.TypeTimeuuid}, {"load", cqlutils.TypeFloat}}},
			wantErr: false,
		},
		{
			args:    `CREATE table users_picture (userid uuid,pictureid uuid,body text,posted_by text,PRIMARY KEY(userid, pictureid, posted_by)) WITH compression = {'sstable_compression': 'LZ4Compressor'};`,
			want:    cqlutils.TableStruct{"users_picture", []cqlutils.ColumnsStruct{{"userid", cqlutils.TypeUuid}, {"pictureid", cqlutils.TypeUuid}, {"body", cqlutils.TypeText}, {"posted_by", cqlutils.TypeText}}},
			wantErr: false,
		},
		{
			args:    `CreATE TABLE data_atrest (pk text PRIMARY KEY,c0 ` + " \t " + ` int) WITH scylla_encryption_options = {'cipher_algorithm' : 'AES/ECB/PKCS5Padding','secret_key_strength' : 128,'key_provider': 'LocalFileSystemKeyProviderFactory','secret_key_file': '/etc/scylla/data_encryption_keys/secret_key'};`,
			want:    cqlutils.TableStruct{"data_atrest", []cqlutils.ColumnsStruct{{"pk", cqlutils.TypeText}, {"c0", cqlutils.TypeInt}}},
			wantErr: false,
		},
		{
			args:    `CREATE Table caching (k int PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`,
			want:    cqlutils.TableStruct{"caching", []cqlutils.ColumnsStruct{{"k", cqlutils.TypeInt}, {"v1", cqlutils.TypeInt}, {"v2", cqlutils.TypeInt}}},
			wantErr: false,
		},
		{
			name:    "without primary key",
			args:    `CREATE Table caching (k int,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`,
			want:    cqlutils.TableStruct{"caching", []cqlutils.ColumnsStruct{{"k", cqlutils.TypeInt}, {"v1", cqlutils.TypeInt}, {"v2", cqlutils.TypeInt}}},
			wantErr: false,
		},
		{
			name:    "no columns",
			args:    `CREATE Table caching () WITH caching = {'enabled': 'true'};`,
			want:    cqlutils.TableStruct{"caching", nil},
			wantErr: false,
		},
		{
			name:    "missing table name",
			args:    `CREATE Table (k int PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`,
			want:    cqlutils.TableStruct{},
			wantErr: true,
		},
		{
			name:    "missing type / name in a column",
			args:    `CREATE Table caching (k PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`,
			want:    cqlutils.TableStruct{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCreateTableStatements(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCreateTableStatements() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseCreateTableStatements() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type BadReader struct{}

func (bd *BadReader) Read(out []byte) (int, error) {
	return 0, fmt.Errorf("")
}

func TestParseTable(t *testing.T) {
	tests := []struct {
		name    string
		args    io.Reader
		want    cqlutils.TableStruct
		wantErr bool
	}{
		{
			name:    "happy path",
			args:    strings.NewReader(`CREATE Table caching (k int PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`),
			want:    cqlutils.TableStruct{"caching", []cqlutils.ColumnsStruct{{"k", cqlutils.TypeInt}, {"v1", cqlutils.TypeInt}, {"v2", cqlutils.TypeInt}}},
			wantErr: false,
		},
		{
			name:    "bad reader",
			args:    &BadReader{},
			want:    cqlutils.TableStruct{},
			wantErr: true,
		},
		{
			name:    "cannot find valid create",
			args:    strings.NewReader(`CREATE Table (k int PRIMARY KEY,v1 int,v2 int,) WITH caching = {'enabled': 'true'};`),
			want:    cqlutils.TableStruct{},
			wantErr: true,
		},
		{
			name:    "cannot parse ",
			args:    strings.NewReader(`CREATE Table caching (k foo PRIMARY KEY,v1 bar,v2 baz,) WITH caching = {'enabled': 'true'};`),
			want:    cqlutils.TableStruct{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTable(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseTable() got = %v, want %v", got, tt.want)
			}
		})
	}
}
