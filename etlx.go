package etlx

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/xingwangc/etlx/driver"
)

// A driver warehouse to register dirvers for different phases and different process.
// Extract and Load map are used for extracting and loading drivers which could be standardized.
// Transform is storing drivers to handle the specifice business.
type EtlDriver struct {
	Extract   map[string]driver.ExtractDriver
	Transform map[string]driver.TransformDriver
	Load      map[string]driver.LoadDriver
}

var (
	drivers  = EtlDriver{}
	driverMu sync.Mutex
)

func init() {
	drivers.Extract = make(map[string]driver.ExtractDriver)
	drivers.Transform = make(map[string]driver.TransformDriver)
	drivers.Load = make(map[string]driver.LoadDriver)
}

//ExtractRegister makes an extract driver available by the provided name.
//If registered twice with the same name of if driver is nil, it panics.
func ExtractRegister(name string, driver driver.ExtractDriver) {
	driverMu.Lock()
	defer driverMu.Unlock()

	if driver == nil {
		panic("exlx: Register driver is nil")
	}
	if _, ok := drivers.Extract[name]; ok {
		panic("etlx: duplicated register extract driver:" + name)
	}
	drivers.Extract[name] = driver
}

//TransformRegister makes a transform driver available by the provided name.
func TransformRegister(name string, driver driver.TransformDriver) {
	driverMu.Lock()
	defer driverMu.Unlock()

	if driver == nil {
		panic("exlx: Register driver is nil")
	}

	if _, ok := drivers.Transform[name]; ok {
		panic("etlx: duplicated register transform driver:" + name)
	}
	drivers.Transform[name] = driver
}

//Load makes a load driver available by the provided name.
func LoadRegister(name string, driver driver.LoadDriver) {
	driverMu.Lock()
	defer driverMu.Unlock()

	if driver == nil {
		panic("load: Register driver is nil")
	}

	if _, ok := drivers.Load[name]; ok {
		panic("load: duplicated register Load driver:" + name)
	}
	drivers.Load[name] = driver
}

//FindExtract is to find an extractor driver specify by its name, return nil if not found
func FindExtract(name string) driver.ExtractDriver {
	drv, ok := drivers.Extract[name]
	if !ok {
		return nil
	}
	return drv
}

//FindTransform is to find a transformer driver specify by its name, return nil if not found
func FindTransform(name string) driver.TransformDriver {
	drv, ok := drivers.Transform[name]
	if !ok {
		return nil
	}
	return drv
}

//FindLoad is to find a loader driver specify by its name, return nil if not found
func FindLoad(name string) driver.LoadDriver {
	drv, ok := drivers.Load[name]
	if !ok {
		return nil
	}
	return drv
}

type ExtractHandler struct {
	Handler driver.Extract
	Arg     interface{}
}

func NewExtract(driverName, name, dataSource string, rawArg []driver.Command) (*ExtractHandler, error) {
	drv := FindExtract(driverName)
	if drv == nil {
		return nil, errors.Errorf("Could not find the extract driver from name %s", driverName)
	}

	handler, err := drv.Open(name, dataSource)
	if err != nil {
		return nil, err
	}

	var arg interface{}
	arg, err = handler.Command(rawArg)
	if err != nil {
		return nil, err
	}

	return &ExtractHandler{Handler: handler, Arg: arg}, nil
}

func (ctx *ExtractHandler) Run() (driver.Rows, error) {
	return ctx.Handler.Query(ctx.Arg)
}

type TransformHandler struct {
	Handler driver.Transform
	Arg     interface{}
}

func NewTransform(driverName, name, dataSource string, rawArg []driver.Command) (*TransformHandler, error) {
	drv := FindTransform(driverName)
	if drv == nil {
		return nil, errors.Errorf("Could not find the transform driver from name %s", driverName)
	}

	handler, err := drv.Open(name, dataSource)
	if err != nil {
		return nil, err
	}

	var arg interface{}
	arg, err = handler.Command(rawArg)
	if err != nil {
		return nil, err
	}

	return &TransformHandler{Handler: handler, Arg: arg}, nil
}

func (ctx *TransformHandler) Run(src driver.Rows) (driver.Results, error) {
	return ctx.Handler.Exec(src, ctx.Arg)
}

type LoadHandler struct {
	Handler driver.Load
	Arg     interface{}
}

func NewLoad(driverName string, name string, dataSource string, rawArg []driver.Command) (*LoadHandler, error) {
	drv := FindLoad(driverName)
	if drv == nil {
		return nil, errors.Errorf("Could not find the load driver from name %s", driverName)
	}

	handler, err := drv.Open(name, dataSource)
	if err != nil {
		return nil, err
	}

	var arg interface{}
	arg, err = handler.Command(rawArg)
	if err != nil {
		return nil, err
	}

	return &LoadHandler{Handler: handler, Arg: arg}, nil
}

func (ctx *LoadHandler) Run(result driver.Results) error {
	return ctx.Handler.Load(result, ctx.Arg)
}
