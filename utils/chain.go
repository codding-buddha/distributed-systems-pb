package utils

import (
	"github.com/juju/errors"
	"sync"
)

type ChainLink struct {
	data interface{}
	id   string
	next *ChainLink
	prev *ChainLink
}

func NewChainLink(data interface{}, id string) *ChainLink {
	return &ChainLink{data: data, id: id, next: &sentinel, prev: &sentinel}
}

func (c *ChainLink) Data() interface{} {
	return c.data
}

func (c *ChainLink) Id() string {
	return c.id
}

func (c *ChainLink) Next() *ChainLink {
	return c.next
}

func (c *ChainLink) Previous() *ChainLink {
	return c.prev
}

func (c *ChainLink) IsHead() bool {
	return c != &sentinel && c.prev == &sentinel
}

func (c *ChainLink) IsTail() bool {
	return c != &sentinel && c.next == &sentinel
}

func (c *ChainLink) IsNull() bool {
	return c == &sentinel
}

// represents empty value
var sentinel = ChainLink{
	data: nil,
	id:   "",
	next: nil,
	prev: nil,
}

type Chain struct {
	head   *ChainLink
	tail   *ChainLink
	lookup map[string]*ChainLink
	lock   sync.RWMutex
}

func NewChain() *Chain {
	return &Chain{
		head:   &sentinel,
		tail:   &sentinel,
		lookup: map[string]*ChainLink{},
		lock:   sync.RWMutex{},
	}
}

func (chain *Chain) isEmpty() bool {
	return *chain.head == sentinel || *chain.tail == sentinel
}

func (chain *Chain) add(item *ChainLink) (*ChainLink, error) {
	prev := &sentinel
	if _, ok := chain.lookup[item.id]; ok {
		return prev, errors.AlreadyExistsf("Item with id %s, already exists", item.id)
	}

	if chain.isEmpty() {
		chain.tail = item
		chain.head = item
	} else {
		prev = chain.tail
		prev.next = item
		item.prev = prev
		chain.tail = item
	}

	chain.lookup[item.id] = item

	return prev, nil
}


func (chain *Chain) IsEmpty() bool {
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	return chain.isEmpty()
}

func (chain *Chain) Add(item *ChainLink) (*ChainLink, error) {
	defer chain.lock.Unlock()
	chain.lock.Lock()
	return chain.add(item)
}

func (chain *Chain) GetById(id string) (*ChainLink, bool) {
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	c, ok := chain.lookup[id]
	return c, ok
}

func (chain *Chain) Count() int {
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	return len(chain.lookup)
}

func (chain *Chain) Clear() {
	defer chain.lock.Unlock()
	chain.lock.Lock()
	curr := chain.head
	for ; curr != &sentinel ; {
		prev := curr
		curr = curr.next
		prev.next = nil
		prev.prev = nil
		curr.prev = nil
	}
	chain.head = &sentinel
	chain.tail = &sentinel
	chain.lookup = make(map[string]*ChainLink)
}


func (chain *Chain) Remove(chainLink *ChainLink) (*ChainLink, error) {
	return chain.RemoveById(chainLink.id)
}

func (chain *Chain) RemoveById(id string) (*ChainLink, error) {
	defer chain.lock.Unlock()
	chain.lock.Lock()
	deleted, ok := chain.lookup[id]

	if !ok {
		return deleted, errors.NotFoundf("Item with id: %s, not found", id)
	}

	// remove reference
	delete(chain.lookup, id)

	prev := deleted.prev
	next := deleted.next


	if *prev != sentinel {
		prev.next = next
	}

	if *next != sentinel {
		next.prev = prev
	}

	if deleted == chain.head {
		chain.head = next
	}

	if deleted == chain.tail {
		chain.tail = prev
	}

	deleted.prev = nil
	deleted.next = nil
	return deleted, nil
}

func (chain *Chain) Head() *ChainLink{
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	return chain.head
}

func (chain *Chain) Tail() *ChainLink {
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	return chain.tail
}

// state should not be changed by apply function
func (chain *Chain) IterFunc(apply func (chain *ChainLink)) {
	defer chain.lock.RUnlock()
	chain.lock.RLock()
	cur := chain.head

	 for ; cur != &sentinel; cur = cur.next {
		apply(cur)
	 }
}

// not thread safe
func (chain *Chain) Iter() func() (*ChainLink, bool) {
	curr := chain.head
	return func() (*ChainLink, bool) {
		if curr == &sentinel {
			return &sentinel, false
		}

		val := curr
		curr = curr.next
		return val, true
	}
}
