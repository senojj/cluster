package graph

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"slices"

	"github.com/golang/groupcache"
	"github.com/jackc/pgx/v5/pgxpool"
)

var TupleRegex = regexp.MustCompile(`([a-z0-9]+):([a-z0-9]+)#([a-z0-9]+)@([a-z0-9]+):([a-z0-9]+)(#([a-z0-9]+))?`)

const QryDirect = `select object_type,
						  object_id,
						  relation,
						  subject_type,
						  subject_id,
						  subject_relation
					from tuples
					where object_type = $1
					and object_id = $2
					and relation = $3
					and subject_type = $4
					and subject_id = $5
					and subject_relation = $6`

const QryUserObjects = `select object_type,
							   object_id,
							   relation,
							   subject_type,
							   subject_id,
							   subject_relation
						from tuples
						where object_type = $1
						and relation = $2
						and subject_type = $3
						and subject_id = $4
						and subject_relation = $5`

const QryObjectUsers = `select object_type,
							   object_id,
							   relation,
							   subject_type,
							   subject_id,
							   subject_relation
						from tuples
						where object_type = $1
						and object_id = $2
						and relation = $3
						and subject_type = $4
						and subject_relation = $5`

type Tuple struct {
	ObjectType      string
	ObjectId        string
	Relation        string
	SubjectType     string
	SubjectId       string
	SubjectRelation string
}

type TuplePart struct {
	Type     string
	Id       string
	Relation string
}

func (t Tuple) Subject() TuplePart {
	return TuplePart{
		Type:     t.SubjectType,
		Id:       t.SubjectId,
		Relation: t.SubjectRelation,
	}
}

func (t Tuple) String() string {
	v := t.ObjectType + ":" + t.ObjectId + "#" + t.Relation + "@" + t.SubjectType + ":" + t.SubjectId
	if t.SubjectRelation != "" {
		v += "#" + t.SubjectRelation
	}
	return v
}

var ErrTuple = errors.New("tuple no match")

func ParseTuple(t string) (Tuple, error) {
	tuple := Tuple{}
	parts := TupleRegex.FindStringSubmatch(t)
	if len(parts) < 6 {
		return tuple, ErrTuple
	}
	tuple.ObjectType = parts[1]
	tuple.ObjectId = parts[2]
	tuple.Relation = parts[3]
	tuple.SubjectType = parts[4]
	tuple.SubjectId = parts[5]
	if len(parts) > 6 {
		tuple.SubjectRelation = parts[7]
	}
	return tuple, nil
}

func Timestamp(t time.Time) string {
	u := t.Unix() / 10
	return strconv.FormatInt(u, 10)
}

func Key(tu Tuple, ti time.Time) string {
	return tu.String() + "|" + Timestamp(ti)
}

type Resolver struct {
	ConnectionPool *pgxpool.Pool
	Group          *groupcache.Group
	Scheduler      *Scheduler
}

func Direct(ctx context.Context, conn *pgxpool.Conn, tu *Tuple) ([]Tuple, error) {
	rows, err := conn.Query(
		ctx,
		QryDirect,
		tu.ObjectType,
		tu.ObjectId,
		tu.Relation,
		tu.SubjectType,
		tu.SubjectId,
		tu.SubjectRelation,
	)
	if err != nil {
		return nil, err
	}
	var tuples []Tuple

	for rows.Next() {
		var tu Tuple
		err := rows.Scan(
			&tu.ObjectType,
			&tu.ObjectId,
			&tu.Relation,
			&tu.SubjectType,
			&tu.SubjectId,
			&tu.SubjectRelation,
		)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tu)
	}

	return tuples, nil
}

func UserObjects(ctx context.Context, conn *pgxpool.Conn, tu *Tuple) ([]Tuple, error) {
	rows, err := conn.Query(
		ctx,
		QryUserObjects,
		tu.ObjectType,
		tu.Relation,
		tu.SubjectType,
		tu.SubjectId,
		tu.SubjectRelation,
	)
	if err != nil {
		return nil, err
	}
	var tuples []Tuple

	for rows.Next() {
		var tu Tuple
		err := rows.Scan(
			&tu.ObjectType,
			&tu.ObjectId,
			&tu.Relation,
			&tu.SubjectType,
			&tu.SubjectId,
			&tu.SubjectRelation,
		)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tu)
	}

	return tuples, nil
}

func ObjectUsers(ctx context.Context, conn *pgxpool.Conn, tu *Tuple) ([]Tuple, error) {
	rows, err := conn.Query(
		ctx,
		QryObjectUsers,
		tu.ObjectType,
		tu.ObjectId,
		tu.Relation,
		tu.SubjectType,
		tu.SubjectRelation,
	)
	if err != nil {
		return nil, err
	}
	var tuples []Tuple

	for rows.Next() {
		var tu Tuple
		err := rows.Scan(
			&tu.ObjectType,
			&tu.ObjectId,
			&tu.Relation,
			&tu.SubjectType,
			&tu.SubjectId,
			&tu.SubjectRelation,
		)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tu)
	}

	return tuples, nil
}

func (r *Resolver) Resolve(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)

	defer func() {
		cancel()
	}()

	splits := strings.Split(key, "|")

	tupleKey, err := ParseTuple(splits[0])
	if err != nil {
		return false, err
	}

	switch tupleKey.ObjectType {
	case "document":
		switch tupleKey.SubjectType {
		case "group":
			if tupleKey.Relation == "viewer" {
				if tupleKey.SubjectRelation == "" {
					chDirect := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
						conn, err := r.ConnectionPool.Acquire(ctx)
						if err != nil {
							return false, err
						}

						in := tupleKey
						in.SubjectRelation = "member"
						defer conn.Release()
						return Direct(ctx, conn, &in)
					})

					chParents := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
						conn, err := r.ConnectionPool.Acquire(ctx)
						if err != nil {
							return nil, err
						}

						in := Tuple{
							ObjectType:  "group",
							Relation:    "parent",
							SubjectType: tupleKey.SubjectType,
							SubjectId:   tupleKey.SubjectId,
						}
						defer conn.Release()
						return UserObjects(ctx, conn, &in)
					})

					select {
					case r := <-chDirect:
						if r.Err != nil {
							return false, r.Err
						}

						if outcome, ok := r.Outcome.([]Tuple); ok && len(outcome) > 0 {
							return true, nil
						}
					case <-ctx.Done():
						return false, ctx.Err()
					}

					var tuples []Tuple

					select {
					case r := <-chParents:
						if r.Err != nil {
							return false, r.Err
						}

						if outcome, ok := r.Outcome.([]Tuple); ok {
							tuples = outcome
						}
					case <-ctx.Done():
						return false, ctx.Err()
					}

					results := make([]<-chan Result, len(tuples))

					for i, tuple := range tuples {
						results[i] = r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
							var answer string
							in := Tuple{
								ObjectType:  tupleKey.ObjectType,
								ObjectId:    tupleKey.ObjectId,
								Relation:    tupleKey.Relation,
								SubjectType: tuple.ObjectType,
								SubjectId:   tuple.ObjectId,
							}
							err := r.Group.Get(ctx, Key(in, time.Now()), groupcache.StringSink(&answer))
							return answer == "true", err
						})
					}

					for {
						if ctx.Err() != nil {
							return false, nil
						}

						if len(results) == 0 {
							return false, nil
						}

						var remove []int

						for i := 0; i < len(results); i++ {
							select {
							case r := <-results[i]:
								if v, ok := r.Outcome.(bool); v && ok {
									cancel()
									return true, nil
								} else if r.Err != nil {
									return false, err
								} else {
									remove = append(remove, i)
								}
							default:
							}
						}

						for _, i := range remove {
							results = slices.Delete(results, i, i+1)
						}
					}
				}
			}
		case "user":
			if tupleKey.Relation == "viewer" {
				chDirect := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					conn, err := r.ConnectionPool.Acquire(ctx)
					if err != nil {
						return false, err
					}
					defer conn.Release()
					return Direct(ctx, conn, &tupleKey)
				})

				chGroups := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					conn, err := r.ConnectionPool.Acquire(ctx)
					if err != nil {
						return nil, err
					}

					in := Tuple{
						ObjectType:      "group",
						Relation:        "member",
						SubjectType:     tupleKey.SubjectType,
						SubjectId:       tupleKey.SubjectId,
						SubjectRelation: tupleKey.SubjectRelation,
					}
					defer conn.Release()
					return UserObjects(ctx, conn, &in)
				})

				select {
				case r := <-chDirect:
					if r.Err != nil {
						return false, r.Err
					}

					if outcome, ok := r.Outcome.([]Tuple); ok && len(outcome) > 0 {
						return true, nil
					}
				case <-ctx.Done():
					return false, ctx.Err()
				}

				var tuples []Tuple

				select {
				case r := <-chGroups:
					if r.Err != nil {
						return false, r.Err
					}

					if outcome, ok := r.Outcome.([]Tuple); ok {
						tuples = outcome
					}
				case <-ctx.Done():
					return false, ctx.Err()
				}

				results := make([]<-chan Result, len(tuples))

				for i, tuple := range tuples {
					results[i] = r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
						var answer string
						in := Tuple{
							ObjectType:  tupleKey.ObjectType,
							ObjectId:    tupleKey.ObjectId,
							Relation:    tupleKey.Relation,
							SubjectType: tuple.ObjectType,
							SubjectId:   tuple.ObjectId,
						}
						err := r.Group.Get(ctx, Key(in, time.Now()), groupcache.StringSink(&answer))
						return answer == "true", err
					})
				}

				for {
					if ctx.Err() != nil {
						return false, nil
					}

					if len(results) == 0 {
						return false, nil
					}

					var remove []int

					for i := 0; i < len(results); i++ {
						select {
						case r := <-results[i]:
							if v, ok := r.Outcome.(bool); v && ok {
								cancel()
								return true, nil
							} else if r.Err != nil {
								return false, err
							} else {
								remove = append(remove, i)
							}
						default:
						}
					}

					for _, i := range remove {
						results = slices.Delete(results, i, i+1)
					}
				}
			}
		}
	}
	return false, nil
}
