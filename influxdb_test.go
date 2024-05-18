package quokka_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/petspalace/quokka"
)

func TestNewInflux(t *testing.T) {
	type test struct {
		input string
		want  *quokka.InfluxMessage
		err   error
	}

	tests := []test{
		{
			input: "temperature,tag=tag-value field=field-value",
			want: &quokka.InfluxMessage{
				Measurement: "temperature",
				TagSet: map[string]string{
					"tag": "tag-value",
				},
				FieldSet: map[string]string{
					"field": "field-value",
				},
				Timestamp: "",
			},
			err: nil,
		},
		{
			input: "temperature,_tag=tag-value field=field-value",
			want:  nil,
			err:   errors.New("Tag key _tag starts with `_` this is not allowed."),
		},
		{
			input: "temperature,tag=tag-value _field=field-value",
			want:  nil,
			err:   errors.New("Field key _field starts with `_` this is not allowed."),
		},
	}

	for _, tc := range tests {
		res, err := quokka.NewInflux(tc.input)

		if !reflect.DeepEqual(err, tc.err) {
			t.Fatalf("expected err: %v got: %v", tc.err, err)
		}

		if !reflect.DeepEqual(tc.want, res) {
			t.Fatalf("expected: %v got: %v", tc.want, res)
		}
	}
}
