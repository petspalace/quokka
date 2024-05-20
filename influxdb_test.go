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
		want  *quokka.InfluxDataPoint
		err   error
	}

	tests := []test{
		// Happy
		{
			input: "temperature,tag=tag-value field=field-value",
			want: &quokka.InfluxDataPoint{
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

		// Reserved key in tags, further tests in `TestInfluxParseSetPart`
		{
			input: "temperature,_tag=tag-value field=field-value",
			want:  nil,
			err:   errors.New("Key _tag starts with `_` this is not allowed."),
		},

		// Reserved key in fields, further tests in `TestInfluxParseSetPart`
		{
			input: "temperature,tag=tag-value _field=field-value",
			want:  nil,
			err:   errors.New("Key _field starts with `_` this is not allowed."),
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

func TestInfluxParseSetPart(t *testing.T) {
	type test struct {
		input string
		want  quokka.InfluxSet
		err   error
	}

	tests := []test{
		// Happy
		{
			input: "tag=tag-value",
			want: quokka.InfluxSet{
				"tag": "tag-value",
			},
			err: nil,
		},
		{
			input: "tag0=tag-value,tag1=tag-value",
			want: quokka.InfluxSet{
				"tag0": "tag-value",
				"tag1": "tag-value",
			},
			err: nil,
		},

		// Reserved keys
		{
			input: "_tag0=tag-value",
			want:  nil,
			err:   errors.New("Key _tag0 starts with `_` this is not allowed."),
		},
		{
			input: "tag0=tag-value,_tag0=tag-value",
			want:  nil,
			err:   errors.New("Key _tag0 starts with `_` this is not allowed."),
		},

		// Duplicate keys
		{
			input: "tag0=tag-value,tag0=tag-value",
			want:  nil,
			err:   errors.New("Key tag0 was already in set this is not allowed."),
		},
		{
			input: "tag0=tag-value,tag1=tag-value,tag0=tag-value",
			want:  nil,
			err:   errors.New("Key tag0 was already in set this is not allowed."),
		},
	}

	for _, tc := range tests {
		res, err := quokka.InfluxParseSetPart(tc.input)

		if !reflect.DeepEqual(err, tc.err) {
			t.Fatalf("expected err: %v got: %v", tc.err, err)
		}

		if !reflect.DeepEqual(tc.want, res) {
			t.Fatalf("expected: %v got: %v", tc.want, res)
		}
	}
}
