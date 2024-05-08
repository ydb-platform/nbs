package common

import (
	"context"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////

type Milestone struct {
	Value               uint32
	ProcessedValueCount uint32
}

////////////////////////////////////////////////////////////////////////////////

type item struct {
	value    uint32
	inflight bool
}

type InflightQueue struct {
	milestone                  Milestone
	milestoneHintForEmptyQueue uint32
	processedValues            <-chan uint32
	holeValues                 ChannelWithCancellation
	inflightLimit              int
	items                      []item
	inflightCount              int
	lastHoleValue              *uint32
	mutex                      sync.RWMutex
	possibleToAdd              Cond
}

// Not thread-safe.
func (q *InflightQueue) Add(ctx context.Context, value uint32) (bool, error) {
	// Wait until hole values are ahead of inflight values.
	if !q.holeValues.Empty() {
		more := true
		for more {
			if q.lastHoleValue != nil && *q.lastHoleValue >= value {
				if *q.lastHoleValue == value {
					return false, nil
				}

				break
			}

			q.mutex.Lock()
			if len(q.items) == 0 {
				// NBS-4141: it is safe to update milestone.
				q.milestone.Value = value
			}
			q.mutex.Unlock()

			var holeValue uint32
			var err error

			holeValue, more, err = q.holeValues.Receive(ctx)
			if err != nil {
				return false, err
			}

			if more {
				q.lastHoleValue = &holeValue
			}
		}
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	for q.inflightCount >= q.inflightLimit {
		err := q.possibleToAdd.Wait(ctx)
		if err != nil {
			return false, err
		}
	}

	q.items = append(
		q.items,
		item{value: value, inflight: true},
	)
	q.inflightCount += 1

	return true, nil
}

func (q *InflightQueue) Milestone() Milestone {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	return q.milestone
}

// Inflight queue uses this hint as milestone value
// if there are no inflight items in the queue.
// If this method is called with some value v,
// values less then v should not be sent to the inflight queue anymore.
func (q *InflightQueue) UpdateMilestoneHintForEmptyQueue(value uint32) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.milestoneHintForEmptyQueue = value
	if len(q.items) == 0 && q.milestoneHintForEmptyQueue > q.milestone.Value {
		q.milestone.Value = q.milestoneHintForEmptyQueue
	}
}

func (q *InflightQueue) Close() {
	q.holeValues.Cancel()
}

////////////////////////////////////////////////////////////////////////////////

func NewInflightQueue(
	milestone Milestone,
	processedValues <-chan uint32,
	holeValues ChannelWithCancellation,
	inflightLimit int,
) *InflightQueue {

	q := &InflightQueue{
		milestone:       milestone,
		processedValues: processedValues,
		holeValues:      holeValues,
		inflightLimit:   inflightLimit,
		items:           make([]item, 0),
	}
	q.possibleToAdd = NewCond(&q.mutex)

	go func() {
		q.drainLoop()
	}()

	return q
}

////////////////////////////////////////////////////////////////////////////////

func (q *InflightQueue) drainLoop() {
	for value := range q.processedValues {
		q.valueProcessed(value)
	}
}

func (q *InflightQueue) valueProcessed(value uint32) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for i := 0; i < len(q.items); i++ {
		if q.items[i].value == value {
			q.items[i].inflight = false
			q.inflightCount -= 1
			q.possibleToAdd.Signal()
			break
		}
	}

	toRemoveCount := 0
	for _, item := range q.items {
		if item.inflight {
			break
		} else {
			toRemoveCount += 1
		}
	}

	q.updateMilestoneOnDrain(toRemoveCount)

	// Remove processed (not in-flight) items from the head.
	q.items = q.items[toRemoveCount:]
}

func (q *InflightQueue) updateMilestoneOnDrain(toRemoveCount int) {
	newMilestoneValue := q.milestone.Value

	if toRemoveCount >= len(q.items) {
		if len(q.items) > 0 {
			lastItemValue := q.items[len(q.items)-1].value
			newMilestoneValue = lastItemValue + 1
		}
		if q.milestoneHintForEmptyQueue > newMilestoneValue {
			newMilestoneValue = q.milestoneHintForEmptyQueue
		}
	} else {
		newMilestoneValue = q.items[toRemoveCount].value
	}

	q.milestone.Value = newMilestoneValue
	q.milestone.ProcessedValueCount += uint32(toRemoveCount)
}
