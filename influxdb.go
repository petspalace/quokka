/* Small Golang library to go to and from the
 * [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).
 *
 * This program will exit on any error, so be sure to run it in an init system
 * or other process manager.
 *
 * This program can also be ran through the use of containers, use either
 * `docker` or `podman`:
 *
 * `podman run -e MQTT_HOST="tcp://127.0.0.1:1883" github.com/petspalace/parrot:latest`
 *
 * This program was made by:
 * - Simon de Vlieger <cmdr@supakeen.com>
 *
 * This program is licensed under the MIT license:
 *
 * Copyright 2022 Simon de Vlieger
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package quokka

import (
	"errors"
	"fmt"
	"strings"
)

/* Describes an InfluxDB Line Protocol message */
type InfluxMessage struct {
	Measurement string
	TagSet      map[string]string
	FieldSet    map[string]string
	Timestamp   string
}

/*
A naive way to parse an InfluxDB Line Protocol message into a struct. This assumes that a tag is always present in the
message while it might not be. It also assumes there are always fields.
*/
func NewInflux(data string) (*InfluxMessage, error) {
	i := InfluxMessage{}

	name, rest, split := strings.Cut(data, ",")

	if !split {
		return nil, errors.New(fmt.Sprintf("Did not find ',' in message '%s'", data))
	}

	if isReserved(name) {
		return nil, errors.New(fmt.Sprintf("Measurement %v starts with `_` this is not allowed.", name))
	}

	i.Measurement = name

	i.TagSet = make(map[string]string)
	i.FieldSet = make(map[string]string)

	tags, fields, _ := strings.Cut(rest, " ")

	for _, tag := range strings.Split(tags, ",") {
		k, v, _ := strings.Cut(tag, "=")

		if isReserved(k) {
			return nil, errors.New(fmt.Sprintf("Tag key %v starts with `_` this is not allowed.", k))
		}

		i.TagSet[k] = v
	}

	for _, field := range strings.Split(fields, ",") {
		k, v, _ := strings.Cut(field, "=")

		if isReserved(k) {
			return nil, errors.New(fmt.Sprintf("Field key %v starts with `_` this is not allowed.", k))
		}

		i.FieldSet[k] = v
	}

	return &i, nil
}

/*
Determine if haystack is reserved, according to the InfluxDB Line Protocol documentation the following
items are reserved:

> Measurement names, tag keys, and field keys cannot begin with an underscore _. The _ namespace is reserved for
> InfluxDB system use.
*/
func isReserved(data string) bool {
	return strings.HasPrefix(data, "_")
}
