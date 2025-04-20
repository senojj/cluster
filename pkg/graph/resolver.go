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

				r.Scheduler.Schedule(ctx, func(ctx context.Context) (bool, error) {
					allowed, err := Direct(ctx, conn, tupleKey)
					if err != nil {
						cancel()
						return false, err
					}

					if allowed {
						cancel()
						return true, nil
					}
					return false, nil
				})

				r.Scheduler.Schedule(ctx, func(ctx context.Context) (bool, error) {
					chIn := make(chan Result, 1)
					ctx, cancel := context.WithCancel(ctx)
					defer func() {
						cancel()
						wgIn.Wait()
						wg.Done()
					}()
					tuples, err := UserObjects(ctx, conn, "group", tupleKey.Subject())
					if err != nil {
						select {
						case chResult <- Result[bool]{
							Err: err,
						}:
						case <-ctx.Done():
						}
						return
					}

					for _, tuple := range tuples {
						if ctx.Err() != nil {
							break
						}
						wgIn.Add(1)
						go func() {
							defer wgIn.Done()

							var dispatchKey Tuple
							dispatchKey.ObjectType = tupleKey.ObjectType
							dispatchKey.ObjectId = tupleKey.ObjectId
							dispatchKey.Relation = tupleKey.Relation
							dispatchKey.SubjectType = tuple.ObjectType
							dispatchKey.SubjectId = tuple.ObjectId
							dispatchKey.SubjectRelation = tuple.Relation

							var dest string
							err := group.Get(ctx, Key(dispatchKey, time.Now()), groupcache.StringSink(&dest))
							if err != nil {
								select {
								case chIn <- Result[bool]{
									Err: err,
								}:
								case <-ctx.Done():
								}
								return
							}

							if dest == "true" {
								select {
								case chIn <- Result[bool]{
									Outcome: true,
								}:
								case <-ctx.Done():
								}
							}
						}()
					}
					select {
					case r := <-chIn:
						chResult <- r
					case <-ctx.Done():
					}
				})

				select {
				case r := <-chResult:
					return r.Outcome, r.Err
				case <-ctx.Done():
				}
			}
		}
	}
	return false, nil
}
