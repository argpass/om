package om

import (
	"errors"
)

// Register a repository, register the repository to `om`
// models are managed by the repository
func Register(repo isRepo) interface{} {
	return repo
}

const (
	LJ int = iota
	RJ
	IJ
)

type joinSpec struct {
	repo isRepo
	joinType int
	on [2]isField
}

type SelectSpec struct {
	err error
	repo isRepo
	joins [] *joinSpec
}

func (s *SelectSpec) On(eqExpr isEqExpr) *SelectSpec {

	f, ok := eqExpr.exprInfo().v.(isField)
	if !ok {
		s.err = errors.New("on expr expect a right value of `isField`")
		return s
	}
	if s.joins == nil {
		s.err = errors.New("on expr should use after join exprs")
		return s
	}
	lastJoin := s.joins[-1]
	if lastJoin.on == nil {
		s.err = errors.New("already set on expr")
		return s
	}
	lastJoin.on = [2]isField{eqExpr.exprInfo().f, f}
	return s
}

func (s *SelectSpec) LJ(other isRepo) *SelectSpec {
	s.joins = append(s.joins, &joinSpec{joinType:LJ, repo:other, on:nil})
	return s
}

func (s *SelectSpec) RJ(other isRepo) *SelectSpec {
	s.joins = append(s.joins, &joinSpec{joinType:RJ, repo:other, on:nil})
	return s
}

func (s *SelectSpec) IJ(other isRepo) *SelectSpec {
	s.joins = append(s.joins, &joinSpec{joinType:IJ, repo:other, on:nil})
	return s
}


type Repo struct {
	Table string
	Managed []interface{}
}

func (r *Repo) LJ(other isRepo) *SelectSpec {
	s := &SelectSpec{err:nil, repo:r, joins:nil}
	return s.LJ(other)
}

func (r *Repo) RJ(other isRepo) *SelectSpec {
	s := &SelectSpec{err:nil, repo:r, joins:nil}
	return s.RJ(other)
}

func (r *Repo) IJ(other isRepo) *SelectSpec {
	s := &SelectSpec{err:nil, repo:r, joins:nil}
	return s.IJ(other)
}

func (r *Repo) getRepo() *Repo {
	return r
}

type isRepo interface {
	getRepo() *Repo
}
