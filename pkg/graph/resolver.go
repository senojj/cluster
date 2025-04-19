package graph

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache"
	"github.com/jackc/pgx/v5/pgxpool"
)

var TupleRegex = regexp.MustCompile(`([a-z0-9]+):([a-z0-9]+)#([a-z0-9]+)@([a-z0-9]+):([a-z0-9]+)(#([a-z0-9]+))?`)

const QryDocumentViewerUser = `select object_type,
									  object_id,
									  relation,
									  subject_type,
									  subject_id,
									  subject_relation
								from tuples
								where object_type = 'document'
								and object_id = $
								and relation = 'viewer'
								and subject_type = 'user'
								and subject_id = $;`

const QryGroupMemberUser = `select object_type,
								   object_id,
								   relation,
								   subject_type,
								   subject_id,
								   subject_relation
							from tuples
							where object_type = 'group'
							and relation = 'member'
							and subject_type = 'user'
							and subject_id = $;`

type Tuple struct {
	ObjectType      string
	ObjectId        string
	Relation        string
	SubjectType     string
	SubjectId       string
	SubjectRelation string
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

type Result struct {
	Outcome bool
	Err     error
}

type Resolver struct {
	ConnectionPool *pgxpool.Pool
	Group          *groupcache.Group
}

func (r *Resolver) Resolve(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	defer func() {
		cancel()
		wg.Wait()
	}()

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
				rows, err := conn.Query(ctx, QryDocumentViewerUser, tupleKey.ObjectId, tupleKey.SubjectId)
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

				rows, err = conn.Query(ctx, QryGroupMemberUser, tupleKey.SubjectId)
				if err != nil {
					return false, err
				}

				for rows.Next() {
					var tu Tuple
					err := rows.Scan(&tu)
					if err != nil {
						return false, err
					}
					tuples = append(tuples, tu)
				}

				ch := make(chan Result)

				for _, tuple := range tuples {
					wg.Add(1)
					go func() {
						defer wg.Done()

						var dispatchKey Tuple
						dispatchKey.ObjectType = tupleKey.ObjectType
						dispatchKey.ObjectId = tupleKey.ObjectId
						dispatchKey.Relation = tupleKey.Relation
						dispatchKey.SubjectType = tuple.ObjectType
						dispatchKey.SubjectId = tuple.ObjectId
						dispatchKey.SubjectRelation = tuple.Relation

						var dest Result
						var buf []byte
						err := group.Get(ctx, Key(dispatchKey, time.Now()), groupcache.AllocatingByteSliceSink(&buf))
						if err != nil {
							dest.Err = err
							select {
							case <-ctx.Done():
							case ch <- dest:
							}
							return
						}
						dec := gob.NewDecoder(bytes.NewReader(buf))
						dec.Decode(&dest)
						select {
						case <-ctx.Done():
						case ch <- dest:
						}
					}()
				}
			}
		}
	}
}
