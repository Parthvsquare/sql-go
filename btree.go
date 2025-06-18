package main

const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

type Node struct {
	keys [][]byte
	// one of the following
	vals [][]byte // for leaf nodes only
	kids []*Node  // for internal nodes only
}

func Encode(node *Node) []byte
func Decode(page []byte) (*Node, error)
