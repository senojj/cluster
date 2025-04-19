package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/golang/groupcache"
	"github.com/hashicorp/memberlist"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MODEL
/*
	type user

	type group
		relations
			define parent: [group]
			define member: [user] or member from parent

	type document
		relations
			define viewer: [user, group#member]
*/

// TUPLES
/*
	group:1#member@user:1
	group:2#parent@group:1
	group:3#parent@group:1
	document:1#viewer@group:2#member
*/

// RESOLUTION SEQUENCE
/*
	1. DISPATCH - document:1#viewer@user:1
	1a. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'document'
				and object_id = '1'
				and relation = 'viewer'
				and subject_type = 'user'
				and subject_id = '1';
	1b. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'group'
				and relation = 'member'
				and subject_type = 'user'
				and subject_id = '1';
	2. DISPATCH - document:1#viewer@group:1#member
	2a. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'document'
				and object_id = '1'
				and relation = 'viewer'
				and subject_type = 'group'
				and subject_id = '1'
				and subject_relation = 'member';
	2b. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'group'
				and relation = 'parent'
				and subject_type = 'group'
				and subject_id = '1';
	3. DISPATCH - document:1#viewer@group:2#member
	3a. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'document'
				and object_id = '1'
				and relation = 'viewer'
				and subject_type = 'group'
				and subject_id = '2'
				and subject_relation = 'member';
	4. DISPATCH - document:1#viewer@group:3#member
	4a. QUERY - select object_type,
					   object_id,
					   relation,
					   subject_type,
					   subject_id,
					   subject_relation
				from tuples
				where object_type = 'document'
				and object_id = '1'
				and relation = 'viewer'
				and subject_type = 'group'
				and subject_id = '3'
				and subject_relation = 'member';
*/

type LoggingTransport struct {
	inner http.RoundTripper
}

func (l *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := l.inner.RoundTrip(req)
	if err != nil {
		log.Println(err)
	}
	return resp, err
}

type MetaDelegate struct {
	Port string
}

func (d *MetaDelegate) NodeMeta(limit int) []byte {
	return []byte(d.Port)
}
func (d *MetaDelegate) LocalState(join bool) []byte                { return nil }
func (d *MetaDelegate) NotifyMsg(msg []byte)                       {}
func (d *MetaDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *MetaDelegate) MergeRemoteState(buf []byte, join bool)     {}

func NewFriends() *friends {
	return &friends{
		m: make(map[string]struct{}),
	}
}

type friends struct {
	m   map[string]struct{}
	mut sync.Mutex
}

func (ms *friends) Add(member string) {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	ms.m[member] = struct{}{}
}

func (ms *friends) Remove(member string) {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	delete(ms.m, member)
}

func (ms *friends) List() []string {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	keys := make([]string, len(ms.m))
	var ctr int
	for member := range ms.m {
		keys[ctr] = member
		ctr++
	}
	return keys
}

func main() {
	args := os.Args
	if len(args) < 3 {
		log.Fatal("not enough arguments")
	}
	prtCluster, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal(err)
	}
	prtCache := args[2]
	delegate := MetaDelegate{
		Port: prtCache,
	}

	conf := memberlist.DefaultLocalConfig()
	conf.AdvertisePort = prtCluster
	conf.BindPort = prtCluster
	conf.Name = args[1]
	conf.Delegate = &delegate

	chEvent := make(chan memberlist.NodeEvent, 1)

	chEventDelegate := memberlist.ChannelEventDelegate{
		Ch: chEvent,
	}

	conf.Events = &chEventDelegate

	list, err := memberlist.Create(conf)
	if err != nil {
		log.Fatal("Failed to create memberlist: " + err.Error())
	}

	addr := "http://" + list.LocalNode().Addr.String() + ":" + prtCache
	srv := list.LocalNode().Addr.String() + ":" + prtCache

	pool := groupcache.NewHTTPPool(addr)

	lt := LoggingTransport{
		http.DefaultTransport,
	}

	pool.Transport = func(ctx context.Context) http.RoundTripper {
		return &lt
	}

	friends := NewFriends()

	go func() {
		for event := range chEvent {
			switch event.Event {
			case memberlist.NodeJoin:
				println(event.Node.Addr.String() + ":" + string(event.Node.Meta))
				friends.Add("http://" + event.Node.Addr.String() + ":" + string(event.Node.Meta))
			case memberlist.NodeLeave:
				friends.Remove("http://" + event.Node.Addr.String() + ":" + string(event.Node.Meta))
			default:
				continue
			}
			ml := friends.List()
			pool.Set(ml...)
		}
	}()

	var mems []string
	if len(args) > 3 {
		mems = args[3:]
		_, err = list.Join(mems)
		if err != nil {
			log.Fatal("Failed to join cluster: " + err.Error())
		}
	}

	for _, member := range list.Members() {
		friends.Add("http://" + member.Addr.String() + ":" + string(member.Meta))
	}

	pool.Set(friends.List()...)

	cpool, err := pgxpool.New(context.Background(), "posgres://127.0.0.1:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}

	group := groupcache.NewGroup("xyz", 64<<20, groupcache.GetterFunc(
		func(ctx context.Context, key string, dest groupcache.Sink) error {

			dest.SetString(prtCache)
			return nil
		},
	))

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	go http.ListenAndServe(srv, nil)

loop:
	for {
		select {
		case <-ch:
			break loop
		case <-time.After(5 * time.Second):
			t := time.Now().Unix() / 10
			s := strconv.FormatInt(t, 10)
			var value string
			e := group.Get(context.Background(), "test"+"|"+s, groupcache.StringSink(&value))
			if e != nil {
				log.Println(e)
			}
			println(value)
		}
	}
	list.Shutdown()
	close(chEvent)
}
