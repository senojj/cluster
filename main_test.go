package main

import (
	"testing"

	"cluster/pkg/graph"

	"github.com/stretchr/testify/require"
)

func TestParseTuple(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		output graph.Tuple
		err    error
	}{
		{
			name:  "valid no subject relation",
			input: "group:1#member@user:1",
			output: graph.Tuple{
				ObjectType:  "group",
				ObjectId:    "1",
				Relation:    "member",
				SubjectType: "user",
				SubjectId:   "1",
			},
			err: nil,
		},
		{
			name:  "valid with subject relation",
			input: "document:1#viewer@group:2#member",
			output: graph.Tuple{
				ObjectType:      "document",
				ObjectId:        "1",
				Relation:        "viewer",
				SubjectType:     "group",
				SubjectId:       "2",
				SubjectRelation: "member",
			},
			err: nil,
		},
		{
			name:   "invalid",
			input:  "document:1#viewer@group",
			output: graph.Tuple{},
			err:    graph.ErrTuple,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tu, err := graph.ParseTuple(tc.input)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.output, tu)
		})
	}
}
