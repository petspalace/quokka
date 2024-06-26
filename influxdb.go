/*
Small Golang library to go to and from the
[InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).

This program was made by:
- Simon de Vlieger <cmdr@supakeen.com>

This program is licensed under the MIT license:

Copyright 2024 Simon de Vlieger

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
*/

package quokka

import (
	"errors"
	"fmt"
	"strings"
)

type InfluxSet map[string]string

/* Describes an InfluxDB Line Protocol message */
type InfluxDataPoint struct {
	Measurement string
	TagSet      InfluxSet
	FieldSet    InfluxSet
	Timestamp   string
}

/*
A naive way to parse an InfluxDB Line Protocol message into a struct. This assumes that a tag is always present in the
message while it might not be. It also assumes there are always fields.
*/
func NewInflux(data string) (*InfluxDataPoint, error) {
	point := InfluxDataPoint{}

	name, rest, split := strings.Cut(data, ",")

	if !split {
		return nil, errors.New(fmt.Sprintf("Did not find ',' in message '%s'", data))
	}

	if InfluxIsReserved(name) {
		return nil, errors.New(fmt.Sprintf("Measurement %v starts with `_` this is not allowed.", name))
	}

	point.Measurement = name
	point.TagSet = make(InfluxSet)
	point.FieldSet = make(InfluxSet)

	tags, fields, _ := strings.Cut(rest, " ")

	var err error

	if point.TagSet, err = InfluxParseSetPart(tags); err != nil {
		return nil, err
	}

	if point.FieldSet, err = InfluxParseSetPart(fields); err != nil {
		return nil, err
	}

	return &point, nil
}

/*
Determine if haystack is reserved, according to the InfluxDB Line Protocol documentation the following
items are reserved:

> Measurement names, tag keys, and field keys cannot begin with an underscore _. The _ namespace is reserved for
> InfluxDB system use.
*/
func InfluxIsReserved(data string) bool {
	return strings.HasPrefix(data, "_")
}

/*
Parsing of InfluxDB Line Protocol sets which are really maps.

The InfluxDB Line Protocol documentation does not state how to handle duplicate keys in maps. `quokka` is strict here
and will error on duplicate keys.
*/
func InfluxParseSetPart(data string) (InfluxSet, error) {
	set := make(InfluxSet)

	for _, field := range strings.Split(data, ",") {
		k, v, _ := strings.Cut(field, "=")

		if _, d := set[k]; d {
			return nil, errors.New(fmt.Sprintf("Key %v was already in set this is not allowed.", k))
		}

		if InfluxIsReserved(k) {
			return nil, errors.New(fmt.Sprintf("Key %v starts with `_` this is not allowed.", k))
		}

		set[k] = v
	}

	return set, nil
}
