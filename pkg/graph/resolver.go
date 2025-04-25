package graph

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

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

func Direct(ctx context.Context, conn *pgxpool.Conn, tu Tuple) (bool, error) {
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
		return false, err
	}
	var tuples []Tuple

	for rows.Next() {
		var tu Tuple
		err := rows.Scan(&tu)
		if err != nil {
			return false, err
		}
		tuples = append(tuples, tu)
	}

	if len(tuples) > 0 {
		return true, nil
	}
	return false, nil
}

func UserObjects(ctx context.Context, conn *pgxpool.Conn, obj string, rel string, sub TuplePart) ([]Tuple, error) {
	rows, err := conn.Query(
		ctx,
		QryUserObjects,
		obj,
		rel,
		sub.Type,
		sub.Id,
		sub.Relation,
	)
	if err != nil {
		return nil, err
	}
	var tuples []Tuple

	for rows.Next() {
		var tu Tuple
		err := rows.Scan(&tu)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tu)
	}
	return tuples, nil
}

func (r *Resolver) Resolve(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)

	println("resolving..... " + key)

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
		case "user":
			if tupleKey.Relation == "viewer" {
				println("resolving viewer")
				chDirect := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					conn, err := r.ConnectionPool.Acquire(ctx)
					if err != nil {
						return false, err
					}
					defer conn.Conn().Close(ctx)
					return Direct(ctx, conn, tupleKey)
				})

				chGroups := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					conn, err := r.ConnectionPool.Acquire(ctx)
					if err != nil {
						return nil, err
					}
					defer conn.Conn().Close(ctx)
					return UserObjects(ctx, conn, "group", "member", tupleKey.Subject())
				})

				select {
				case r := <-chDirect:
					if r.Err != nil {
						return false, r.Err
					}

					if outcome, ok := r.Outcome.(bool); ok && outcome {
						return true, nil
					}
				case <-ctx.Done():
					return false, ctx.Err()
				}
				println("direct...")

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
				println("userset...")

				results := make([]<-chan Result, len(tuples))

				for i, tuple := range tuples {
					results[i] = r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
						var answer string
						err := r.Group.Get(ctx, Key(tuple, time.Now()), groupcache.StringSink(&answer))
						return answer == "true", err
					})
				}

				for {
					println("looping...")
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
						results = slices.Delete(results, i, i)
					}
				}
			}
		}
	}
	return false, nil
}
