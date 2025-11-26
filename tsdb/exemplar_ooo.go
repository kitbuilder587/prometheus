// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"github.com/prometheus/prometheus/model/exemplar"
)

// findInsertPosition finds the correct position to insert an OOO exemplar in the doubly linked list.
// Returns the buffer index where the exemplar should be inserted before.
// Returns noExemplar if the exemplar should be appended at the end.
func (ce *CircularExemplarStorage) findInsertPosition(idx *indexEntry, e exemplar.Exemplar) int {
	if idx == nil {
		return noExemplar
	}

	currentIdx := idx.oldest
	for {
		current := &ce.exemplars[currentIdx]
		cmp := exemplar.Compare(e, current.exemplar)

		if cmp <= 0 {
			return currentIdx
		}

		if current.next == noExemplar {
			return noExemplar
		}

		currentIdx = current.next
	}
}

// insertExemplarOOO inserts an exemplar at a specific position in the doubly linked list.
func (ce *CircularExemplarStorage) insertExemplarOOO(idx *indexEntry, e exemplar.Exemplar, insertBeforeIdx int) {
	newSlot := ce.nextIndex

	if evicted := &ce.exemplars[newSlot]; evicted.ref != nil {
		ce.evictExemplar(evicted)
	}

	if insertBeforeIdx == noExemplar {
		ce.appendAtEnd(idx, e, newSlot)
	} else {
		ce.insertInMiddle(idx, e, newSlot, insertBeforeIdx)
	}

	ce.nextIndex = (ce.nextIndex + 1) % len(ce.exemplars)
}

func (ce *CircularExemplarStorage) evictExemplar(evicted *circularBufferEntry) {
	if evicted.next == noExemplar {
		var buf [1024]byte
		evictedLabels := evicted.ref.seriesLabels.Bytes(buf[:])
		delete(ce.index, string(evictedLabels))
	} else {
		evicted.ref.oldest = evicted.next
		ce.exemplars[evicted.next].prev = noExemplar
	}
}

func (ce *CircularExemplarStorage) appendAtEnd(idx *indexEntry, e exemplar.Exemplar, newSlot int) {
	if idx.newest != newSlot {
		ce.exemplars[idx.newest].next = newSlot
		ce.exemplars[newSlot].prev = idx.newest
	} else {
		ce.exemplars[newSlot].prev = noExemplar
	}

	ce.exemplars[newSlot].next = noExemplar
	ce.exemplars[newSlot].exemplar = e
	ce.exemplars[newSlot].ref = idx
	idx.newest = newSlot
}

func (ce *CircularExemplarStorage) insertInMiddle(idx *indexEntry, e exemplar.Exemplar, newSlot, insertBeforeIdx int) {
	insertBefore := &ce.exemplars[insertBeforeIdx]
	prevIdx := insertBefore.prev

	ce.exemplars[newSlot].exemplar = e
	ce.exemplars[newSlot].ref = idx
	ce.exemplars[newSlot].next = insertBeforeIdx
	ce.exemplars[newSlot].prev = prevIdx

	insertBefore.prev = newSlot

	if prevIdx == noExemplar {
		idx.oldest = newSlot
	} else {
		ce.exemplars[prevIdx].next = newSlot
	}
}
