package quokka_test

import (
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
			},
			err: nil,
		},
	}

	for _, tc := range tests {
		res, err := quokka.NewInflux(tc.input)

		if err != tc.err {
			t.Fatalf("expected err: %v got: %v", tc.want, err)
		}

		if !reflect.DeepEqual(tc.want, res) {
			t.Fatalf("expected: %v got: %v", tc.want, res)
		}
	}
}
