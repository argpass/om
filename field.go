package om

type Table struct {
}

type isTable interface {
}

type exprInfo struct {
	f *isField
	v interface{}
	op String
}

func (expr *exprInfo) exprInfo() *exprInfo  {
	return expr
}

type isExpr interface {
	exprInfo() *exprInfo
}

type eqExpr struct {
	exprInfo
}

func (eq *eqExpr) eqExpr()  *eqExpr {
	return eq
}

type isEqExpr interface {
	isExpr
	eqExpr() *eqExpr
}

type isField interface {
	FieldInfo() *Field
}

type Field struct {
	Column string
	Null bool
	IsPK bool
}

func (f *Field) Eq(v interface{}) isEqExpr {
	return &eqExpr{exprInfo:exprInfo{f:f, v:v}}
}

func (f *Field) FieldInfo() *Field {
	return f
}

type String struct {
	Field

	Default string
	MaxLen int
}

type Integer struct {
	Field

	Default int
	Max int
	Min int
}

type ForeignKey struct {
	F isField

	RelTo isTable
	RelField isField
}

func (f *ForeignKey) FieldInfo() *Field {
	return f.F.FieldInfo()
}

func (f *ForeignKey) Eq(v interface{}) isEqExpr {
	return nil
}
