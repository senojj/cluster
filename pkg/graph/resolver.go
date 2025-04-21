package graph

import (
	"context"
	"errors"
	"regexp"
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
					where object_type = $
					and object_id = $
					and relation = $
					and subject_type = $
					and subject_id = $
					and subject_relation = $;`

const QryUserObjects = `select object_type,
							   object_id,
							   relation,
							   subject_type,
							   subject_id,
							   subject_relation
						from tuples
						where object_type = $
						and relation = $
						and subject_type = $
						and subject_id = $
						and subject_relation = $;`

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

func UserObjects(ctx context.Context, conn *pgxpool.Conn, obj string, sub TuplePart) ([]Tuple, error) {
	rows, err := conn.Query(
		ctx,
		QryUserObjects,
		obj,
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

	defer func() {
		cancel()
	}()

	chResult := make(chan Result, 1)

	splits := strings.Split(key, "|")

	tupleKey, err := ParseTuple(splits[0])
	if err != nil {
		return false, err
	}

	group := groupcache.GetGroup("xyz")
	if group == nil {
		return false, errors.New("no group")
	}

	conn, err := r.ConnectionPool.Acquire(ctx)
	if err != nil {
		return false, err
	}

	switch tupleKey.ObjectType {
	case "document":
		switch tupleKey.SubjectType {
		case "user":
			if tupleKey.Relation == "viewer" {
				ctx, cancel := context.WithCancel(ctx)

				chDirect := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					return Direct(ctx, conn, tupleKey)
				})

				chGroups := r.Scheduler.Schedule(ctx, func(ctx context.Context) (any, error) {
					return UserObjects(ctx, conn, "group", tupleKey.Subject())
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

				for _, tuple := range tuples {

				}
			}
		}
	}
	return false, nil
}
