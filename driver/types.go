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
	"gopkg.in/mgo.v2/bson"
)

const (
	PointLngIndex int = iota
	PointLatIndex
)

type Point [2]float64 //0-lng, 1-lat
type LineString []Point
type MultiPoint []Point
type Polygon []MultiPoint
type MultiLineString []LineString
type MultiPolygon []Polygon
type Geometry struct {
	Type        string      `json:"type" bson:"type" map:"type"`
	Coordinates interface{} `json:"coordinates" bson:"coordinates" map:"coordinates"`
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
	case float64:
		return strconv.FormatFloat(val.(float64), 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(val.(float32)), 'f', -1, 32), nil
	case []uint8:
		return string(val.([]uint8)), nil
	case time.Time:
		valt := val.(time.Time)
		return valt.String(), nil
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
	case string:
		strval := strings.ToLower(val.(string))
		switch strval {
		case "1", "true", "t", "y", "yes", "是":
			return true, nil
		case "0", "false", "f", "n", "no", "否", "不是":
			return false, nil
		}
	}
	return false, fmt.Errorf("Interface(%v) could not be converted to Bool!\n", val)
}

//ParseFloat converts a string to float number, it supports scientific notiation and comma seperated number
func ParseFloat(str string) (float64, error) {
	val, err := strconv.ParseFloat(str, 64)
	if err == nil {
		return val, nil
	}

	//Some number may be seperated by comma, for example, 23,120,123, so remove the comma firstly
	str = strings.Replace(str, ",", "", -1)

	//Some number is specifed in scientific notation
	pos := strings.IndexAny(str, "eE")
	if pos < 0 {
		return strconv.ParseFloat(str, 64)
	}

	var baseVal float64
	var expVal int64

	baseStr := str[0:pos]
	baseVal, err = strconv.ParseFloat(baseStr, 64)
	if err != nil {
		return 0, err
	}

	expStr := str[(pos + 1):]
	expVal, err = strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return baseVal * math.Pow10(int(expVal)), nil
}

func IntFromInterface(val interface{}) (int64, error) {
	if nil == val {
		return 0, fmt.Errorf("Interface(%v) could not be converted to Int!\n", val)
	}

	switch val.(type) {
	case float64: //round the float
		fval := val.(float64)
		var ival int64
		if fval >= 0 {
			ival = int64(fval + 0.5)
		} else {
			ival = int64(fval - 0.5)
		}
		return ival, nil
	case int64:
		return val.(int64), nil
	case int:
		return int64(val.(int)), nil
	case string:
		if fval, err := ParseFloat(val.(string)); err == nil {
			return int64(fval), nil
		}
	}
	return 0, fmt.Errorf("Interface(value=%v, type=%v) could not be converted to Int!", val, reflect.TypeOf(val))
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
		if fval, err := ParseFloat(val.(string)); err == nil {
			return fval, nil
		}
	}
	return 0.0, fmt.Errorf("Interface(value=%v, type=%v) Could not be converted to Float!\n", val, reflect.TypeOf(val))
}

//convert the time format from 2006-1-2 to 2006-01-02 or 2006/1/2 to 2006/01/02
func formatTime(val string, format string) (string, error) {
	rslt := val
	splitStr := "-"
	if regexp.MustCompile("[0-9]+-[0-9]+-[0-9]").MatchString(val) {
		splitStr = "-"
	} else if regexp.MustCompile("[0-9]+/[0-9]+/[0-9]").MatchString(val) {
		splitStr = "/"
	} else if regexp.MustCompile("[0-9]+\\.[0-9]+\\.[0-9]").MatchString(val) {
		splitStr = "."
	} else {
		return val, nil
	}

	items := strings.Split(val, splitStr)
	if len(items) != 3 {
		return "", fmt.Errorf("wrong time format:", val)
	}

	if splitStr == "." {
		splitStr = "-"
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

func splitTimeValueLayout(input string) (val, layout string) {
	const seperator = "::"
	results := strings.Split(input, seperator)
	if len(results) > 1 {
		return results[0], results[1]
	} else {
		return results[0], ""
	}
}

//layout string indicating which format of time to return. it could be:
//	layout: "20160102"
//		"2006-01-02"
//		"2006/01/02"
func TimeFromInterface(val interface{}, layout string) (time.Time, error) {
	var err error
	if nil == val {
		return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
	}
	switch val.(type) {
	case time.Time:
		return val.(time.Time), nil
	case int:
		return time.Unix(int64(val.(int)), 0), nil
	case string:
		timeStr, embedLayout := splitTimeValueLayout(val.(string))
		if len(embedLayout) <= 0 {
			timeStr, err = formatTime(timeStr, layout)
			if err != nil {
				return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
			}
		} else {
			layout = embedLayout
		}
		// timestr, err := formatTime(val.(string), layout)
		// if err != nil {
		// 	return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
		// }

		tval, err := time.ParseInLocation(layout, timeStr, time.Local)
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
	case string:
		m := make(map[string]interface{})
		err := json.Unmarshal([]byte(val.(string)), &m)
		return m, err
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

func BsonRegExFromInterface(val interface{}) (bson.RegEx, error) {
	if nil == val {
		return bson.RegEx{}, fmt.Errorf("Interface(%v) could not be converted to bson.RegEx!\n", val)
	}

	reg := []interface{}{}
	switch val.(type) {
	case bson.RegEx:
		return val.(bson.RegEx), nil
	case []interface{}:
		reg = val.([]interface{})
	default:
		return bson.RegEx{}, fmt.Errorf("Interface(%v) could not be converted to bson.RegEx!\n", val)
	}

	if len(reg) == 0 || len(reg) > 2 {
		return bson.RegEx{}, fmt.Errorf("Interface(%v) to convert to bson.RegEx should be a [] has at leasta pattern !\n", val)
	} else if len(reg) == 1 {
		pattern, err := StringFromInterface(reg[0])
		if err != nil {
			return bson.RegEx{}, fmt.Errorf("pattern to bson.RegEx should be a string !\n", val)
		}
		return bson.RegEx{pattern, ""}, nil
	} else {
		pattern, err := StringFromInterface(reg[0])
		if err != nil {
			return bson.RegEx{}, fmt.Errorf("pattern to bson.RegEx should be a string !\n", val)
		}
		option, err := StringFromInterface(reg[0])
		if err != nil {
			return bson.RegEx{}, fmt.Errorf("option to bson.RegEx should be a string !\n", val)
		}
		return bson.RegEx{pattern, option}, nil
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

func initInterfaceInMap(obj map[string]interface{}) {
	for key, item := range obj {
		switch item.(type) {
		case map[string]interface{}:
			initInterfaceInMap(item.(map[string]interface{}))
		case []interface{}:
			obj[key] = []interface{}{}
		default:
			obj[key] = nil
		}
	}
}

func setMapValue(obj map[string]interface{}, key string, value interface{}) error {
	for k, item := range obj {
		if k == key {
			obj[k] = value
			return nil
		} else {
			switch item.(type) {
			case map[string]interface{}:
				err := setMapValue(item.(map[string]interface{}), key, value)
				if err == nil {
					return nil
				}
			default:
				continue
			}
		}
	}
	return fmt.Errorf("Did not find the key:%s in the template!", key)
}

func removeNilValueFromMap(obj map[string]interface{}) {
	for key, value := range obj {
		if value == nil {
			delete(obj, key)
		} else {
			switch value.(type) {
			case map[string]interface{}:
				if len(value.(map[string]interface{})) == 0 {
					delete(obj, key)
				} else {
					removeNilValueFromMap(value.(map[string]interface{}))
				}
			case []interface{}:
				if len(value.([]interface{})) == 0 {
					delete(obj, key)
				} else {
					for _, item := range value.([]interface{}) {
						switch item.(type) {
						case map[string]interface{}:
							removeNilValueFromMap(item.(map[string]interface{}))
						}
					}
				}
			}
		}
	}
}

func JsonFromMap(src map[string]interface{}, columns []string, template map[string]interface{}) ([]byte, error) {
	container := template

	if len(container) == 0 {
		container = make(map[string]interface{})
		for _, key := range columns {
			if value, ok := src[key]; ok {
				container[key] = value
			} else {
				container[key] = nil
			}
		}
	} else {
		//initialize all fields of the template to nil
		initInterfaceInMap(container)

		for _, key := range columns {
			if value, ok := src[key]; ok {
				err := setMapValue(container, key, value)
				if err != nil {
					return []byte{}, err
				}
			}
		}
	}

	removeNilValueFromMap(container)
	//do twice to remove empty map
	removeNilValueFromMap(container)

	return json.Marshal(container)
}

func StrToType(typeStr string, src interface{}, layout ...string) (dst interface{}, err error) {
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
		if len(layout) > 0 {
			return TimeFromInterface(src, layout[0])
		}
		return TimeFromInterface(src, "2006-01-02")
	case "geometry":
		return GeometryFromInterface(src)
	case "bool":
		return BoolFromInterface(src)
	case "bson.RegEx":
		return BsonRegExFromInterface(src)
	case "json":
		return json.Marshal(src)
	case "jsonarray":
		return json.Marshal(src)
	default:
		return nil, fmt.Errorf("The type(%s) is not supported right now", typeStr)
	}
}

//Define a common comman arguments for any objects.
type Command struct {
	Name  string           `json:"name"`
	Type  string           `json:"type"`
	Value interface{}      `json:"value"`
	Arg   *json.RawMessage `json:"arg"`
}

//unmarshal the json string to command
//This will be invoked when execute json.Unmarshal() to unmarshal the string into Command
func (cmds *Command) UnmarshalJSON(b []byte) error {
	var tmp struct {
		CmdType  string           `json:"name"`
		ItemType string           `json:"type"`
		Items    *json.RawMessage `json:"value"`
		Arg      *json.RawMessage `json:"arg"`
	}

	err := yaml.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}

	cmds.Name = tmp.CmdType
	cmds.Type = tmp.ItemType
	cmds.Arg = tmp.Arg

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
	} else if "json" == tmp.ItemType {
		items := map[string]interface{}{}
		err := yaml.Unmarshal(*(tmp.Items), &items)
		if err != nil {
			return err
		}
		cmds.Value = items
		return nil
	} else if "jsonarray" == tmp.ItemType {
		var items []map[string]interface{}
		err := yaml.Unmarshal(*(tmp.Items), &items)
		if err != nil {
			return err
		}
		cmds.Value = items
		return nil
	} else if "raw" == tmp.ItemType {
		cmds.Value = string(*tmp.Items)
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
	//if len(columns) < len(contents) {
	//	return nil, fmt.Errorf("length of columns should >= length of contents!")
	//}

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

//transform a struct to map. The struct should define the map field
func StructToMap(src interface{}) (rslt map[string]interface{}, err error) {
	rslt = make(map[string]interface{})

	t := reflect.TypeOf(src)
	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i).Tag.Get("map")
		rslt[key] = reflect.ValueOf(src).Field(i).Interface()
	}
	return rslt, nil
}

//transform a struct to array.
func StructToArray(columns []string, src interface{}) (rslt []interface{}, err error) {

	tmpMap, err := StructToMap(src)
	if err != nil {
		return rslt, err
	}

	return MapToArray(columns, tmpMap)
}

//transform map to a structure
func MapToStructure(src map[string]interface{}, obj interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()

	t := structValue.Type()
	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i).Tag.Get("map")

		fieldToSet := structValue.Field(i)
		if !fieldToSet.IsValid() {
			return fmt.Errorf("Field: [%s] is invalid!", key)
		}

		if !fieldToSet.CanSet() {
			return fmt.Errorf("Field: [%s] is not setable!", key)
		}

		if value, ok := src[key]; ok {
			rfValue := reflect.ValueOf(value)
			if fieldToSet.Type() != rfValue.Type() {
				rfValue = rfValue.Convert(fieldToSet.Type())
			}
			fieldToSet.Set(rfValue)
		}
	}

	return nil
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

//NamePlace identify the index of the specificed filed in the array
type NamePlace struct {
	Name string
	//for regex, split the Dst is the index of value which will be
	//set to destination.
	//for replace, this field is not used.
	//for mapping, the Dst is the value which is used to recover the source value
	Dst interface{}
}

type StrProcessor struct {
	//name of the string to be processed in a map
	SrcName string
	//defined the name corresponding to which value in the result array
	DstDescriptor []NamePlace

	//right now support regexp and split
	Command string
	//if command is regexp, this is the expression.
	//if the command is split, this is the seperator
	ProcDescriptor string
}

//Provide a way to set command
func (strproc *StrProcessor) SetCommand(cmd string) {
	strproc.Command = cmd
}

func (strproc StrProcessor) regexProc(srcStr string) (map[string]interface{}, error) {
	rslt := make(map[string]interface{})

	re := regexp.MustCompile(strproc.ProcDescriptor)

	tmp := re.FindAllString(srcStr, -1)
	tmplength := len(tmp)
	for _, dst := range strproc.DstDescriptor {
		var index int
		if i, ok := dst.Dst.(int64); ok {
			index = int(i)
		} else {
			rslt[dst.Name] = nil
			continue
		}

		if tmplength == 0 || srcStr == "" || index >= tmplength {
			rslt[dst.Name] = nil
		} else if index < 0 {
			//-100 to indicate return the result list directly
			if index == -100 && len(strproc.DstDescriptor) == 1 {
				rslt[dst.Name] = tmp
			} else {
				rslt[dst.Name] = tmp[tmplength+index]
			}
		} else {
			rslt[dst.Name] = tmp[index]
		}
	}

	return rslt, nil
}

//split processing will trim the space for the result by default.
func (strproc StrProcessor) splitProc(srcStr string) (map[string]interface{}, error) {
	rslt := make(map[string]interface{})

	tmp := strings.Split(srcStr, strproc.ProcDescriptor)
	tmplength := len(tmp)
	for _, dst := range strproc.DstDescriptor {
		var index int
		if i, ok := dst.Dst.(int64); ok {
			index = int(i)
		} else {
			rslt[dst.Name] = nil
			continue
		}

		if tmplength == 0 || srcStr == "" || index >= tmplength {
			rslt[dst.Name] = nil
		} else if index < 0 {
			//-100 to indicate return the result list directly
			if index == -100 && len(strproc.DstDescriptor) == 1 {
				rslt[dst.Name] = tmp
			} else {
				rslt[dst.Name] = strings.TrimSpace(tmp[tmplength+index])
			}
		} else {
			rslt[dst.Name] = strings.TrimSpace(tmp[index])
		}
	}

	return rslt, nil
}

//replace processing is reusing the command configuration of regex and split.
//For replace processing the dst field is at 0 index in dst array.
func (strproc StrProcessor) replaceProc(srcStr string) (map[string]interface{}, error) {
	rslt := make(map[string]interface{})

	tmp := strings.Split(strproc.ProcDescriptor, "|")
	if len(tmp) != 2 {
		return map[string]interface{}{}, fmt.Errorf("The ProcDescriptor[%s] has a wrong format!", strproc.ProcDescriptor)
	}
	old := tmp[0]
	new := tmp[1]

	newStr := strings.Replace(srcStr, old, new, -1)
	rslt[strproc.DstDescriptor[0].Name] = newStr

	return rslt, nil
}

//function "Process" to process a string with predefined configuration.
//It support 2 parameters, the 1st one is the string which would be processing.
//If the 1st string parameter is empty, it will use the predefined SrcName to
//find out the string to be processing in the second parameter with map[sting]
//type.
//
//Just support only 2 kinds of command right now, one is regex and the other one
//is split.
func (strproc StrProcessor) Process(srcStr string, srcMap map[string]interface{}) (map[string]interface{}, error) {
	strObj := ""
	if len(srcStr) > 0 {
		strObj = srcStr
	} else if str, ok := srcMap[strproc.SrcName]; ok {
		obj, err := StringFromInterface(str)
		if err != nil {
			return map[string]interface{}{strproc.SrcName: nil}, err
		}
		strObj = obj
	} else {
		//do not return empty map, use the empty string to make sure a map returned with dest field set to nil
		strObj = ""
	}

	switch strproc.Command {
	case "regex":
		return strproc.regexProc(strObj)
	case "split":
		return strproc.splitProc(strObj)
	case "replace":
		return strproc.replaceProc(strObj)
	default:
		return map[string]interface{}{}, fmt.Errorf("The processor was initialized with an unsupported command: [%s]", strproc.Command)
	}
}

//Batch opreation suppoted
type Batch struct {
	Flag   bool //true enable, false: disable
	Limit  int64
	Offset int64
}

func (bt *Batch) SetBatch(limit, offset int64) {
	bt.Flag = true
	bt.Limit = limit
	bt.Offset = offset
}

type Table struct {
	data    [][]interface{}
	columns []string
	cursor  int
}

func NewTable(capacity int) *Table {
	var data [][]interface{}
	if capacity > 0 {
		data = make([][]interface{}, 0, capacity)
	} else {
		data = make([][]interface{}, 0)
	}
	return &Table{data: data}
}

func NewTableFromMap(mapArr []map[string]interface{}) *Table {
	tbl := NewTable(len(mapArr))

	if mapArr == nil || len(mapArr) <= 0 {
		return tbl
	}

	//extract the columns from the first element
	columns := make([]string, 0, len(mapArr[0]))
	for key := range mapArr[0] {
		columns = append(columns, key)
	}
	tbl.SetColumns(columns)

	//extract all data following the order of columns
	for _, m := range mapArr {
		data := make([]interface{}, len(columns), len(columns))
		for cid, cname := range columns {
			data[cid] = m[cname]
		}
		tbl.AppendData(data)
	}

	return tbl
}

func (t *Table) Close() error {
	return nil
}

func (t *Table) SetColumns(cols []string) {
	t.columns = cols
}

func (t *Table) SetData(data [][]interface{}) {
	t.data = data
}

func (t *Table) GetData() [][]interface{} {
	return t.data
}

func (t *Table) AppendData(data []interface{}) {
	t.data = append(t.data, data)
}

func (t *Table) ResetCurosr() {
	t.cursor = 0
}

func (t *Table) Columns() []string {
	return t.columns
}

func (t *Table) Next(dst interface{}) error {
	if t.cursor >= len(t.data) {
		return fmt.Errorf("Reach the end of table")
	}
	if value, ok := dst.(**[]interface{}); ok {
		*value = &t.data[t.cursor]
	} else if value, ok := dst.(*interface{}); ok {
		*value = t.data[t.cursor]
	} else if value, ok := dst.([]interface{}); ok {
		for i, k := range t.data[t.cursor] {
			value[i] = k
		}
	} else {
		fmt.Println("could not copy next in sql,", value)
	}
	t.cursor++
	return nil
}
