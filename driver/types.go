package driver

import (
	"fmt"
	//	"reflect"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/ghodss/yaml"
)

func StringFromInterface(val interface{}) (string, error) {
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
	switch val.(type) {
	case bool:
		return val.(bool), nil
	}
	return false, fmt.Errorf("Interface(%v) could not be converted to Bool!\n", val)
}

func IntFromInterface(val interface{}) (int64, error) {
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

//layout string indicating which format of time to return. it could be:
//	layout: "20160102"
//		"2006-01-02"
//		"2006/01/02"
func TimeFromInterface(val interface{}, layout string) (time.Time, error) {
	fmt.Println("time", val)
	switch val.(type) {
	case time.Time:
		return val.(time.Time), nil
	case int:
		return time.Unix(int64(val.(int)), 0), nil
	case string:
		tval, err := time.Parse(layout, val.(string))
		if err == nil {
			return tval, err
		}
	}
	return time.Now(), fmt.Errorf("Interface(%v) could not be converted to Time!\n", val)
}

func MapFromInterface(val interface{}) (map[string]interface{}, error) {
	switch val.(type) {
	case map[string]interface{}:
		return val.(map[string]interface{}), nil
	}

	return map[string]interface{}{}, fmt.Errorf("Interface(%v) could not be converted to map[string]interface{}!\n", val)
}

func ArrayFromInterface(val interface{}) ([]interface{}, error) {
	switch v := val.(type) {
	case []interface{}:
		return v, nil
	case *[]interface{}:
		return *v, nil
	}

	return []interface{}{}, fmt.Errorf("Interface(%v) could not be converted to []interface{}!\n", val)
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
		fmt.Println("dst:", dst)
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
	BatchSize int    `json:"batch_size"`
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
