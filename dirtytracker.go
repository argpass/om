package om

import (
	"reflect"
	"fmt"
	"errors"
)

// Tracking struct is the base flag to make a struct be tracked
//
// What fields can be tracked ?
// 1.Comparable Types
// 2.Implement the Comparable interface
// 3.un exported field won't be tracked
//
// Example:
//
//	type Car struct {
//              // put tracking tag
//		om.Tracking
//
//              // un exported field won't be tracked
//		id int
//		Age int
//		Name string
//		No uint64
//		Email Email
//
//              // Comparable interface can be tracked
//		Address *Address
//		Parent *Parent
//		Data []string
//	}
//
type Tracking struct {
	tracker
}

// Comparable interface can be tracked,
// so any customer type can be tracked by implementing the interface
//
// Example:
//      // The struct Address can be used as tracking field in a tracking struct
//	type Address struct {
//		EmailA Email
//		EmailB Email
//	}
//
//      // implement `Comparable` interface
//	func (a *Address) Equal(v interface{}) bool {
//		fmt.Println("go", a, v)
//		if a == nil || v == nil{
//			return v == a
//		}
//		v2 := v.(*Address)
//		return a.EmailA == v2.EmailA && a.EmailB == v2.EmailB
//	}
//
type Comparable interface {
	Equal(interface{})bool
}

type fieldInfo struct {
	fieldStruct reflect.StructField
	fieldValue reflect.Value
	isComparable bool
}

func (f *fieldInfo) Name() string {
	return f.fieldStruct.Name
}

func (f *fieldInfo) Val() interface{} {
	return f.fieldValue.Interface()
}

type isDirtyTracker interface {
	// track starts a watch point to begin tracking
	track(isDirtyTracker) error
	// dirtyFields returns dirty fields map `{field_name}=>{field_value}`
	dirtyFields() (map[string]interface{}, error)
}

type tracker struct {
	holdType reflect.Type
	snapshot map[string]interface{}
	target   isDirtyTracker
	members  [] *fieldInfo
}

// track starts a watch point to begin tracking
func (t *tracker) track(target isDirtyTracker) error  {
	// target must be pointer type,
	// so i can track changes on it with initial
	tp := reflect.ValueOf(target)
	if ! tp.IsValid() {
		return fmt.Errorf("invalid target:%+v", target)
	}
	if tp.Kind() != reflect.Ptr {
		return fmt.Errorf("only can track ptr, got %v", tp.Kind())
	}
	tp = reflect.Indirect(tp)
	te := tp.Type()

	t.target = target
	// type info
	if t.holdType != te {
		t.holdType = te
		t.members = nil
		for i := 0; i < tp.NumField(); i++ {
			fieldValue := tp.Field(i)
			fieldStruct := te.Field(i)
			// never track un exported field
			if !fieldValue.CanInterface() {
				continue
			}
			face := fieldValue.Interface()
			if _, ok := face.(Comparable); ok {
				t.members = append(t.members,
					&fieldInfo{fieldStruct, fieldValue, true})
				continue
			}
			if fieldValue.Type().Comparable() {
				t.members = append(t.members,
					&fieldInfo{fieldStruct, fieldValue, false})
			}
		}
	}
	t.newSnapshot()
	return nil
}

// dirtyFields returns dirty fields map `{field_name}=>{field_value}`
// return value may be nil
func (t *tracker) dirtyFields() (map[string]interface{}, error) {
	if t.snapshot == nil {
		return nil, errors.New("make snapshot firtly")
	}
	var dirty = map[string]interface{}{}
	for _, info := range t.members {
		v := info.Val()
		if info.isComparable {
			if !v.(Comparable).Equal(t.snapshot[info.Name()]) {
				dirty[info.Name()] = v
			}
		}else{
			if t.snapshot[info.Name()] != v {
				dirty[info.Name()] = v
			}
		}
	}
	return dirty, nil
}

func (t *tracker) newSnapshot() {
	// new snapshot
	t.snapshot = make(map[string]interface{})
	for _, info := range t.members {
		t.snapshot[info.Name()] = info.Val()
	}
}

func Watch(t isDirtyTracker)  {
	t.track(t)
}

func Debug(t isDirtyTracker)  {
	fmt.Printf("\ndebug>>%+v\n", t)
}

func GetDirtyFields(t isDirtyTracker) map[string]interface{} {
	dirty, _ := t.dirtyFields()
	return dirty
}
