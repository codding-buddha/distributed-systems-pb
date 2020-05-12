package utils

import (
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func arrangeChain() *utils.Chain {
	chain := utils.NewChain()
	chain.Add(utils.NewChainLink("v1", "v1"))
	chain.Add(utils.NewChainLink("v2", "v2"))
	chain.Add(utils.NewChainLink("v3", "v3"))
	chain.Add(utils.NewChainLink("v4", "v4"))
	chain.Add(utils.NewChainLink("v5", "v5"))
	return chain
}

func TestRemoveHeadOfTheChain(t *testing.T) {
	chain := arrangeChain()
	cases := []struct{
		delete, newHead, newTail string
	} {
		{"v1", "v2", "v5"},
		{"v2", "v3", "v5"},
		{"v3", "v4", "v5"},
		{"v4", "v5", "v5"},
		{"v5","", ""},
	}

	for _, c :=  range cases {
		chain.RemoveById(c.delete)
		head := ""
		tail := ""
		if chain.Head().Data() != nil {
			head = chain.Head().Data().(string)
		}
		if chain.Tail().Data() != nil {
			tail = chain.Tail().Data().(string)
		}

		assert.Equal(t, c.newHead, head, "Incorrect newHead after removal")
		assert.Equal(t, c.newTail, tail, "Incorrect tail after removal")
	}

	assert.True(t, chain.IsEmpty())
	assert.True(t, chain.Head().IsNull())
}

func TestRemoveByIdWithAlreadyRemovedKey(t *testing.T) {
	chain := arrangeChain()
	_, err := chain.RemoveById("v1")
	assert.Equal(t, nil, err)
	_, err = chain.RemoveById("v1")
	assert.True(t, errors.IsNotFound(err), "Expected not found error, but got " + err.Error())
}

func TestRemoveFromTail(t *testing.T) {
	chain := arrangeChain()
	cases := []struct{
		delete, newHead, newTail string
	} {
		{"v5", "v1", "v4"},
		{"v4", "v1", "v3"},
		{"v3", "v1", "v2"},
		{"v2", "v1", "v1"},
		{"v1","", ""},
	}

	for _, c :=  range cases {
		chain.RemoveById(c.delete)
		head := ""
		tail := ""
		if chain.Head().Data() != nil {
			head = chain.Head().Data().(string)
		}
		if chain.Tail().Data() != nil {
			tail = chain.Tail().Data().(string)
		}

		assert.Equal(t, c.newHead, head, "Incorrect newHead after removal")
		assert.Equal(t, c.newTail, tail, "Incorrect tail after removal")
	}

	assert.True(t, chain.IsEmpty())
	assert.True(t, chain.Tail().IsNull())
}