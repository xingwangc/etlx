package driver

//UnquotedString is a special string that don't need to be quoted during sql building
type UnquotedString struct {
	Value string
}

//NewUnquotedString is the factory function to return a unquoted version string from input string literal
func NewUnquotedString(val string) *UnquotedString {
	result := &UnquotedString{Value: val}
	return result
}

func (us UnquotedString) String() string {
	return us.Value
}

//Interface of extract driver
type ExtractDriver interface {
	Open(name string, dataSource string) (Extract, error)
}

//Interface of transfrom driver
type TransformDriver interface {
	Open(name string, dataSource string) (Transform, error)
}

//Interface of load driver
type LoadDriver interface {
	Open(name string, dataSource string) (Load, error)
}

//Interface of extract handler
type Extract interface {
	//SetBatch should be called before Command to make sure the setting is valid
	SetBatch(limit int64, offset int64)
	Command(args []Command) (cmd interface{}, _ error)
	Query(cmd interface{}) (Rows, error)
	Close() error
}

//Interface of transform handler
type Transform interface {
	Command(args []Command) (cmd interface{}, _ error)
	Exec(src Rows, cmd interface{}) (Results, error)
	Close() error
}

//Interface of load handler
type Load interface {
	Command(args []Command) (cmd interface{}, _ error)
	Load(src Results, cmd interface{}) error
	QueryFromNextStep() (Rows, error)
	Close() error
}

//Interface to iterate rows
type Rows interface {
	Close() error
	Next(dst interface{}) error
	Columns() []string
}

//Interface to iterate results. for transform results to load into MongoDB, it is special.
//Mostly it is like the rows.
type Results interface {
	Rows
	NextRsltAndIndex(rslt interface{}, index *map[string]interface{}) error
}
