/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over the skiplist object. Use Skiplist.NewIterator
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list *Skiplist
	nd   *node
}

// Close resets the iterator.
func (it *Iterator) Close() error {
	it.list = nil
	it.nd = nil
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.nd != it.list.head && it.nd != it.list.tail
}

// SeekGE moves the iterator to the first entry whose key is greater than or
// equal to the given key. Returns true if the given key exists and false
// otherwise.
func (it *Iterator) SeekGE(key []byte) (found bool) {
	_, it.nd, found = it.seekForBaseSplice(key)
	return found
}

// SeekLE moves the iterator to the first entry whose key is less than or equal
// to the given key. Returns true if the given key exists and false otherwise.
func (it *Iterator) SeekLE(key []byte) (found bool) {
	var prev, next *node
	prev, next, found = it.seekForBaseSplice(key)

	if found {
		it.nd = next
	} else {
		it.nd = prev
	}

	return found
}

// First seeks position at the first entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (it *Iterator) First() bool {
	it.nd = it.list.getNext(it.list.head, 0)
	return it.nd != it.list.tail
}

// Last seeks position at the last entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (it *Iterator) Last() bool {
	it.nd = it.list.getPrev(it.list.tail, 0)
	return it.nd != it.list.head
}

// Next advances to the next position. If there are no following nodes, then
// Valid() will be false after this call.
func (it *Iterator) Next() bool {
	it.nd = it.list.getNext(it.nd, 0)
	return it.nd != it.list.tail
}

// Prev moves to the previous position. If there are no previous nodes, then
// Valid() will be false after this call.
func (it *Iterator) Prev() bool {
	it.nd = it.list.getPrev(it.nd, 0)
	return it.nd != it.list.head
}

// Key returns the key at the current position.
func (it *Iterator) Key() []byte {
	return it.nd.getKey(it.list.arena)
}

// Value returns the value at the current position.
func (it *Iterator) Value() []byte {
	return it.nd.getValue(it.list.arena)
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	level := int(it.list.Height() - 1)

	prev = it.list.head
	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)

		if found {
			break
		}

		if level == 0 {
			break
		}

		level--
	}

	return
}

// IsSameArray returns true if the slices are the same length and the array
// underlying the two slices is the same. Always returns false for empty arrays.
func isSameArray(val1, val2 []byte) bool {
	if len(val1) == len(val2) && len(val1) > 0 {
		return &val1[0] == &val2[0]
	}

	return false
}
