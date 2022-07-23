package scheduler

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

var fn = func(ctx context.Context) {}

func TestAddDurationScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add one key", func(t *testing.T) {
			t.Parallel()
			key := "add#1"
			duration := 1 * time.Millisecond
			schedule := NewScheduler()
			err := schedule.Add(key, duration, fn)
			assert.Nil(t, err)
			isExists, tm := schedule.read(key)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, tm)
			time.Sleep(duration * 2)
			isExists, tm = schedule.read(key)
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
		})

		t.Run("Add multiple key", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 1000))
			for i := 1; i <= 1000; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.Add(key, 10*time.Millisecond, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, tm := schedule.read(keyRandom)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, tm)
			time.Sleep(20 * time.Millisecond)
			isExists, tm = schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add data and check key is not exist", func(t *testing.T) {
			t.Parallel()
			key := "add#1"
			duration := 1 * time.Millisecond
			schedule := NewScheduler()
			err := schedule.Add(key, duration, fn)
			assert.Nil(t, err)
			isExists, tm := schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
			time.Sleep(duration * 2)
			isExists, tm = schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
		})

		t.Run("Key is exists", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			for i := 1; i <= 5; i++ {
				key := fmt.Sprintf("add#%d", i)
				duration := time.Duration(i) * time.Millisecond
				err := schedule.Add(key, duration, fn)
				assert.Nil(t, err)
			}

			err := schedule.Add("add#5", 1*time.Millisecond, func(ctx context.Context) {})
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsExists)
			time.Sleep(10 * time.Millisecond)
			isExists, tm := schedule.read("add#5")
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
		})

		t.Run("Add multiple key and key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			for i := 1; i <= 1000; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.Add(key, 10*time.Millisecond, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, tm := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, tm)
		})
	})
}
