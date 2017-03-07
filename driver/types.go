package driver

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ghodss/yaml"
)

type Point [2]float64 //0-lng, 1-lat
type LineString []Point
type MultiPoint []Point
type Polygon []MultiPoint
type MultiLineString []LineString
type MultiPolygon []Polygon
type Geometry struct {
	Type        string      `json:"type" bson:"type"`
	Coordinates interface{} `json:"coordinates" bson"coordinates"`
}

type GeometryCollection struct {
	Type       string     `json:"type" bson:"type"`
	Geometries []Geometry `json:"geometries" bson:"geometries"`
}

func (geom *Geometry) UnmarshalJSON(b []byte) error {
	var tmp struct {
		Type        string           `json:"type"`
		Coordinates *json.RawMessage `json:"coordinates"`
	}

	err := yaml.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}

	var coordinates interface{}
	geom.Type = tmp.Type
	switch tmp.Type {
	case "Point":
		coordinates = Point{}
	case "LineString":
		coordinates = LineString{}
	case "Polygon":
		coordinates = Polygon{}
	case "MultiPoint":
		coordinates = MultiPoint{}
	case "MultiLineString":
		coordinates = MultiLineString{}
	case "MultiPolygon":
		coordinates = MultiPolygon{}
	}
	err = yaml.Unmarshal(*tmp.Coordinates, &coordinates)
	if err != nil {
		return err
	}
	geom.Coordinates = coordinates
	return nil
}

func StringFromInterface(val interface{}) (string, error) {
	if nil == val {
		return "", fmt.Errorf("Interface(%v) could not be converted to String!\n", val)
	}
	switch val.(type) {
	case string:
		return val.(string), nil
	case int:
		return strconv.Itoa(val.(int)), nil
	case int64:
		return strconv.FormatInt(val.(int64), 10), nil
	case []uint8:
		return string(val.([]uint8)), nil
	}
	return "", fmt.Errorf("Interface(%v) could not be converted to String!\n", val)
}

func BoolFromInterface(val interface{}) (bool, error) {
	if nil == val {
		return false, fmt.Errorf("Interface(%v) could not be converted to Bool!\n", val)
	}
	switch val.(type) {
	case bool:
		return val.(bool), nil
	}
	return false, fmt.Errorf("Interface(%v) could not be converted to Bool!\n", val)
}

func IntFromInterface(val interface{}) (int64, error) {
	if nil == val {
		return 0, fmt.Errorf("Interface(%v) could not be converted to Int!\n", val)
	}

	switch val.(type) {
	case float64:
		return int64(val.(float64)), nil
	case int64:
		return val.(int64), nil
	case int:
		return int64(val.(int)), nil
	case string:
		ival, err := strconv.ParseInt(val.(string), 10, 64)
		if err == nil {
			return ival, err
		}
	}
	return 0, fmt.Errorf("Interface(%v) could not be converted to Int!\n", val)
}

func FloatFromInterface(val interface{}) (float64, error) {
	if nil == val {
		return 0.0, fmt.Errorf("Interface(%v) Could not be converted to Float!\n", val)
	}
	switch val.(type) {
	case float64:
		return val.(float64), nil
	case float32:
		return float64(val.(float32)), nil
	case int:
		return float64(val.(int)), nil
	case int64:
		return float64(val.(int64)), nil
	case string:
		fval, err := strconv.ParseFloat(val.(string), 64)
		if err == nil {
			return fval, err
		}
	}
	return 0.0, fmt.Errorf("Interface(%v) Could not be converted to Float!\n", val)
}

//convert the time format from 2006-1-2 to 2006-01-02 or 2006/1/2 to 2006/01/02
func formatTime(val string, format string) (string, error) {
	rslt := val
	splitStr := "-"
	if regexp.MustCompile("[0-9]+-[0-9]+-[0-9]").MatchString(val) {
		splitStr = "-"
	} else if regexp.MustCompile("[0-9]+/[0-9]+/[0-9]").MatchString(val) {
		splitStr = "/"
	} else {
		return val, nil
	}

	items := strings.Split(val, splitStr)
	if len(items) != 3 {
		return "", fmt.Errorf("wrong time format:", val)
	}

	rslt = items[0]
	if len(items[1]) == 1 {
		rslt = rslt + splitStr + "0" + items[1]
	} else {
		rslt = rslt + splitStr + items[1]
	}

	if len(items[2]) == 1 {
		rslt = rslt + splitStr + "0" + items[2]
	} else {
		rslt = rslt + splitStr + items[2]
	}

	return rslt, nil
}

//layout string indicating which format of time to return. it could be:
//	layout: "20160102"
//		"2006-01-02"
//		"2006/01/02"
func TimeFromInterface(val interface{}, layout string) (time.Time, error) {
	if nil == val {
		return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
	}
	switch val.(type) {
	case time.Time:
		return val.(time.Time), nil
	case int:
		return time.Unix(int64(val.(int)), 0), nil
	case string:
		timestr, err := formatTime(val.(string), layout)
		if err != nil {
			return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
		}

		tval, err := time.Parse(layout, timestr)
		//tval, err := time.Parse(layout, val.(string))
		if err == nil {
			return tval, err
		}
	}
	return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
}

func MapFromInterface(val interface{}) (map[string]interface{}, error) {
	if nil == val {
		return map[string]interface{}{}, fmt.Errorf("Interface(%v) could not be converted to map[string]interface{}!\n", val)
	}
	switch val.(type) {
	case map[string]interface{}:
		return val.(map[string]interface{}), nil
	}

	return map[string]interface{}{}, fmt.Errorf("Interface(%v) could not be converted to map[string]interface{}!\n", val)
}

func ArrayFromInterface(val interface{}) ([]interface{}, error) {
	if nil == val {
		return []interface{}{}, fmt.Errorf("Interface(%v) could not be converted to []interface{}!\n", val)
	}
	switch v := val.(type) {
	case []interface{}:
		return v, nil
	case *[]interface{}:
		return *v, nil
	}

	return []interface{}{}, fmt.Errorf("Interface(%v) could not be converted to []interface{}!\n", val)
}

func GeometryFromInterface(val interface{}) (Geometry, error) {
	if nil == val {
		return Geometry{}, fmt.Errorf("Interface(%v) could not be converted to Geometry!\n", val)
	}

	switch val.(type) {
	case Geometry:
		return val.(Geometry), nil
	case string:
		geometry := Geometry{}
		err := json.Unmarshal([]byte(val.(string)), &geometry)
		return geometry, err
	case []byte:
		geometry := Geometry{}
		err := json.Unmarshal(val.([]byte), &geometry)
		return geometry, err
	default:
		return Geometry{}, fmt.Errorf("Interface(%v) could not be converted to Geometry!\n", val)
	}
}

func CopyValue(src interface{}, dst interface{}) error {
	var err error = nil

	switch d := dst.(type) {
	case *string:
		*d, err = StringFromInterface(src)
	case *int64:
		*d, err = IntFromInterface(src)
	case *bool:
		*d, err = BoolFromInterface(src)
	case *map[string]interface{}:
		*d, err = MapFromInterface(src)
	case *[]interface{}:
		slice, err := ArrayFromInterface(src)
		if err != nil {
			return err
		}
		for _, value := range slice {
			*d = append(*d, value)
		}
	default:
		err = fmt.Errorf("Usupported type of destination")
	}

	return err
}

func StrToType(typeStr string, src interface{}) (dst interface{}, err error) {
	switch typeStr {
	case "int":
		return IntFromInterface(src)
	case "string":
		return StringFromInterface(src)
	case "float":
		return FloatFromInterface(src)
	case "list", "array":
		return ArrayFromInterface(src)
	case "map":
		return MapFromInterface(src)
	case "time":
		return TimeFromInterface(src, "2006-01-02")
	case "geometry":
		return GeometryFromInterface(src)
	default:
		return nil, fmt.Errorf("The type(%s) is not supported right now", typeStr)
	}
}

//Define a common comman arguments for any objects.
type Command struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

//unmarshal the json string to command
//This will be invoked when execute json.Unmarshal() to unmarshal the string into Command
func (cmds *Command) UnmarshalJSON(b []byte) error {
	var tmp struct {
		CmdType  string           `json:"name"`
		ItemType string           `json:"type"`
		Items    *json.RawMessage `json:"value"`
	}

	err := yaml.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}

	cmds.Name = tmp.CmdType
	cmds.Type = tmp.ItemType

	if "complex" == tmp.ItemType {
		items := []Command{}
		err := yaml.Unmarshal(*(tmp.Items), &items)
		if err != nil {
			return err
		}
		cmds.Value = items
		return nil
	} else if "single" == tmp.ItemType {
		items := []Command{}
		err := yaml.Unmarshal(*(tmp.Items), &items)
		if err != nil {
			return err
		}
		cmds.Value = items
		return nil
	} else if tmp.ItemType != "" && tmp.Items != nil {
		var src interface{}
		err := yaml.Unmarshal(*(tmp.Items), &src)
		if err != nil {
			return err
		}
		item, err := StrToType(tmp.ItemType, src)
		if err != nil {
			return err
		}
		cmds.Value = item
	}

	return nil
}

//Construction is the Construction function of Command.
//It connvert the src structure to a the ETLX defined command args format.
//The src structure which could be connvert to command should have the silimar structure
// and should has the item with the same json tag name with command
func (cmd *Command) Construction(src interface{}) error {
	return nil
}

type BatchStruct struct {
	BatchSize int64  `json:"batch_size"`
	BatchCtl  string `json:"batch_control"`
}

func Round(src float64, n int) float64 {
	pow10 := math.Pow10(n)

	return math.Trunc((src+0.5/pow10)*pow10) / pow10
}

//pre-process datas, data read from postgres, if the data is string, then it will be returned
//as []uint8. Int, float would not be changed, for some cases we could not use the []uint8
//type to process strings, so convert []unint8 to string here.
func DataPreProcess(src interface{}) interface{} {
	switch src.(type) {
	case []uint8:
		return string(src.([]uint8))
	default:
		return src
	}
}

//transform a columns array and contents array to a map
func ArrayToMap(columns []string, contents []interface{}) (rslt map[string]interface{}, err error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("Should provide a columns to define the return sequence!")
	}
	if len(columns) < len(contents) {
		return nil, fmt.Errorf("length of columns should >= length of contents!")
	}
	result := make(map[string]interface{})
	for index, key := range columns {
		result[key] = contents[index]
	}

	return result, nil
}

//transform a map to an array with sequence defined in the columns passed in
func MapToArray(columns []string, contents map[string]interface{}) (rslt []interface{}, err error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("Should provide a columns to define the return sequence!")
	}
	if len(columns) < len(contents) {
		return nil, fmt.Errorf("length of columns should >= length of contents!")
	}

	result := make([]interface{}, len(columns))
	for index, key := range columns {
		if value, ok := contents[key]; ok {
			result[index] = value
		} else {
			result[index] = nil
		}
	}

	return result, nil
}

//format map[string]interface{}
//Sometime data read from sql was not with the format it is, e.g. string in sql
//would be regturned as []byte. So is most case using those data we should
//convert them back first
func FormatMap(obj map[string]interface{}) {
	for key, value := range obj {
		if value != nil {
			content := reflect.ValueOf(value)
			switch content.Kind() {
			case reflect.Slice:
				obj[key] = string(content.Bytes())
			default:
				obj[key] = content.Interface()
			}
		}
	}
}
