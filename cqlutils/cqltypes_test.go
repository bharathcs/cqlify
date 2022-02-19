package cqlutils

import "testing"

func TestColumnsStruct_String(t *testing.T) {
	type fields struct {
		Name string
		Type CqlNativeType
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				"foo", TypeSmallint,
			},
			want: "foo (smallint)",
		},
		{
			fields: fields{
				"foo", TypeBoolean,
			},
			want: "foo (boolean)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ColumnsStruct{
				Name: tt.fields.Name,
				Type: tt.fields.Type,
			}
			if got := c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCqlNativeType_String(t *testing.T) {
	const invalidTypeString = "INVALID CQL TYPE"
	tests := []struct {
		name       string
		nativeType CqlNativeType
		want       string
	}{
		{
			name:       "valid",
			nativeType: TypeAscii,
			want:       "ascii",
		},
		{
			name:       "invalid",
			nativeType: 10101,
			want:       invalidTypeString,
		},
		{
			name:       "invalid",
			nativeType: -1,
			want:       invalidTypeString,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nativeType.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCqlNativeType(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    CqlNativeType
		wantErr bool
	}{
		{
			name:    "valid",
			args:    "ascii",
			want:    TypeAscii,
			wantErr: false,
		},
		{
			name:    "invalid",
			args:    "invalid",
			want:    CqlNativeType(-1),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetCqlNativeType(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCqlNativeType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetCqlNativeType() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTableStruct_String(t1 *testing.T) {
	type fields struct {
		TableName string
		Columns   []ColumnsStruct
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "1 col",
			fields: fields{
				TableName: "foo",
				Columns:   []ColumnsStruct{{"bar", TypeInt}},
			},
			want: "foo: {bar (int)}",
		},
		{
			name: "2 cols",
			fields: fields{
				TableName: "foo",
				Columns:   []ColumnsStruct{{"bar", TypeInt}, {"baz", TypeInt}},
			},
			want: "foo: {bar (int), baz (int)}",
		},
		{
			name: "empty cols",
			fields: fields{
				TableName: "foo",
				Columns:   nil,
			},
			want: "foo: {}",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := TableStruct{
				TableName: tt.fields.TableName,
				Columns:   tt.fields.Columns,
			}
			if got := t.String(); got != tt.want {
				t1.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestTableStruct_PrettyString(t1 *testing.T) {
	type fields struct {
		TableName string
		Columns   []ColumnsStruct
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "1 col",
			fields: fields{
				TableName: "foo",
				Columns:   []ColumnsStruct{{"bar", TypeInt}},
			},
			want: "foo: {\nbar  int\n}",
		},
		{
			name: "2 cols",
			fields: fields{
				TableName: "foo",
				Columns:   []ColumnsStruct{{"bar", TypeInt}, {"baz", TypeInt}},
			},
			want: "foo: {\nbar  int,\nbaz  int\n}",
		},
		{
			name: "2 unevenly long cols",
			fields: fields{
				TableName: "foo",
				Columns:   []ColumnsStruct{{"barbaric", TypeInt}, {"baz", TypeInt}},
			},
			want: "foo: {\nbarbaric  int,\nbaz       int\n}",
		},
		{
			name: "empty cols",
			fields: fields{
				TableName: "foo",
				Columns:   nil,
			},
			want: "foo: {}",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := TableStruct{
				TableName: tt.fields.TableName,
				Columns:   tt.fields.Columns,
			}
			if got := t.PrettyString(); got != tt.want {
				t1.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
