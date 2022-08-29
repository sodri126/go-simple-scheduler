package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
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
			duration := 500 * time.Millisecond
			schedule := NewScheduler()
			err := schedule.Add(key, duration, fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read(key)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			time.Sleep(750 * time.Millisecond)
			isExists, ds = schedule.read(key)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add multiple key", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 100))
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					key := fmt.Sprintf("add#%d", index)
					err := schedule.Add(key, 500*time.Millisecond, fn)
					assert.Nil(t, err)
					wg.Done()
				}(i)
			}

			wg.Wait()
			isExists, ds := schedule.read(keyRandom)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			time.Sleep(750 * time.Millisecond)
			isExists, ds = schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add data and check key is not exist", func(t *testing.T) {
			t.Parallel()
			key := "add#1"
			duration := 100 * time.Millisecond
			schedule := NewScheduler()
			err := schedule.Add(key, duration, fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
			time.Sleep(duration * 2)
			isExists, ds = schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Key is exists", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			for i := 1; i <= 5; i++ {
				key := fmt.Sprintf("add#%d", i)
				duration := time.Duration(i*100) * time.Millisecond
				err := schedule.Add(key, duration, fn)
				assert.Nil(t, err)
			}

			err := schedule.Add("add#5", 1*time.Millisecond, func(ctx context.Context) {})
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsExists)
			time.Sleep(750 * time.Millisecond)
			isExists, ds := schedule.read("add#5")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add multiple key and key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.Add(key, 500*time.Millisecond, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})
}

func TestAddDateTimeScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add one key", func(t *testing.T) {
			t.Parallel()
			key := "add#1"
			duration := time.Now().UTC().Add(500 * time.Millisecond)
			schedule := NewScheduler()
			err := schedule.AddDate(key, duration, fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read(key)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			time.Sleep(750 * time.Millisecond)
			isExists, ds = schedule.read(key)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add multiple key", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 1000))
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.AddDate(key, time.Now().UTC().Add(500*time.Millisecond), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, ds := schedule.read(keyRandom)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			time.Sleep(750 * time.Millisecond)
			isExists, ds = schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add data and check key is not exist", func(t *testing.T) {
			t.Parallel()
			key := "add#1"
			duration := 100 * time.Millisecond
			schedule := NewScheduler()
			err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
			time.Sleep(duration * 2)
			isExists, ds = schedule.read("add#2")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Key is exists", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			for i := 1; i <= 5; i++ {
				key := fmt.Sprintf("add#%d", i)
				duration := time.Duration(i) * time.Millisecond
				err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
				assert.Nil(t, err)
			}

			err := schedule.AddDate("add#5", time.Now().UTC().Add(500*time.Millisecond), func(ctx context.Context) {})
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsExists)
			time.Sleep(750 * time.Millisecond)
			isExists, ds := schedule.read("add#5")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add multiple key and key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.AddDate(key, time.Now().UTC().Add(500*time.Millisecond), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add key and time is already passed", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			err := schedule.AddDate("add#1", time.Now().UTC().Add(-24*time.Hour), fn)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrDateTimeLessThanNow)
		})
	})
}

func TestCancelScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add multiple key and cancel one key", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 1000))
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.AddDate(key, time.Now().UTC().Add(500*time.Millisecond), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Cancel(keyRandom)
			assert.Nil(t, err)
			isExists, ds := schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add multiple key and key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.AddDate(key, time.Now().UTC().Add(500*time.Millisecond), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Cancel("add#1001")
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
		})

		t.Run("Add multiple key and cancel twice", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 1000))
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.AddDate(key, time.Now().UTC().Add(500*time.Millisecond), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Cancel(keyRandom)
			assert.Nil(t, err)
			isExists, ds := schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
			err = schedule.Cancel(keyRandom)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
		})
	})
}

func TestRescheduleScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add multiple key and reschedule some keys", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			randomNo := randomNumber(1, 1000)
			randomList := make(map[int]string)
			for i := 1; i <= randomNo; i++ {
				randomList[i] = fmt.Sprintf("add#%d", i)
			}

			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.Add(key, duration, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			for _, value := range randomList {
				isExists, ds := schedule.read(value)
				assert.Equal(t, isExists, true)
				assert.NotNil(t, ds)
				err := schedule.Reschedule(value, 500*time.Millisecond)
				assert.Nil(t, err)
			}

			time.Sleep(1 * time.Second)
			for _, key := range randomList {
				isExists, ds := schedule.read(key)
				assert.Equal(t, isExists, false)
				assert.Nil(t, ds)
			}
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add multiple key and reschedule key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.Add(key, duration, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Reschedule("add#1001", 700*time.Millisecond)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})
}

func TestRescheduleDateTimeScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add multiple key and reschedule some keys", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			randomNo := randomNumber(1, 1000)
			randomList := make(map[int]string)
			for i := 1; i <= randomNo; i++ {
				randomList[i] = fmt.Sprintf("add#%d", i)
			}

			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			for _, value := range randomList {
				isExists, ds := schedule.read(value)
				assert.Equal(t, isExists, true)
				assert.NotNil(t, ds)
				err := schedule.RescheduleDateTime(value, time.Now().UTC().Add(500*time.Millisecond))
				assert.Nil(t, err)
			}

			time.Sleep(1 * time.Second)
			for _, key := range randomList {
				isExists, ds := schedule.read(key)
				assert.Equal(t, isExists, false)
				assert.Nil(t, ds)
			}
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add multiple key and reschedule key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Reschedule("add#1001", 750*time.Millisecond)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add key and reschedule time that has passed", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			err := schedule.AddDate("add#1", time.Now().UTC().Add(24*time.Hour), fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read("add#1")
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			err = schedule.RescheduleDateTime("add#1", time.Now().UTC().Add(-24*time.Hour))
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrDateTimeLessThanNow)
		})
	})
}

func TestReplaceScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add multiple key and replace one scheduler", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			keyRandom := fmt.Sprintf("add#%d", randomNumber(1, 1000))
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					err := schedule.Add(key, 500*time.Millisecond, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			isExists, ds := schedule.read(keyRandom)
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			err := schedule.Replace(keyRandom, 650*time.Millisecond, fn)
			assert.Nil(t, err)
			time.Sleep(750 * time.Millisecond)
			isExists, ds = schedule.read(keyRandom)
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add multiple key and replace key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 10 * time.Millisecond
					err := schedule.Add(key, duration, fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.Replace("add#1001", 20*time.Millisecond, fn)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})
	})
}

func TestReplaceDateTimeScheduler(t *testing.T) {
	t.Run("Positive Case", func(t *testing.T) {
		t.Run("Add multiple key and replace some keys", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			randomNo := randomNumber(1, 1000)
			randomList := make(map[int]string)
			for i := 1; i <= randomNo; i++ {
				randomList[i] = fmt.Sprintf("add#%d", i)
			}

			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			for _, value := range randomList {
				isExists, ds := schedule.read(value)
				assert.Equal(t, isExists, true)
				assert.NotNil(t, ds)
				err := schedule.ReplaceDateTime(value, time.Now().UTC().Add(500*time.Millisecond), fn)
				assert.Nil(t, err)
			}

			time.Sleep(1 * time.Second)
			for _, key := range randomList {
				isExists, ds := schedule.read(key)
				assert.Equal(t, isExists, false)
				assert.Nil(t, ds)
			}
		})
	})

	t.Run("Negative Case", func(t *testing.T) {
		t.Run("Add multiple key and replace key is not exists", func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			schedule := NewScheduler()
			wg.Add(1000)
			for i := 1; i <= 1000; i++ {
				go func(index int) {
					defer wg.Done()
					key := fmt.Sprintf("add#%d", index)
					duration := 500 * time.Millisecond
					err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
					assert.Nil(t, err)
				}(i)
			}

			wg.Wait()
			err := schedule.ReplaceDateTime("add#1001", time.Now().UTC().Add(600*time.Millisecond), fn)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyIsNotExists)
			isExists, ds := schedule.read("add#1001")
			assert.Equal(t, isExists, false)
			assert.Nil(t, ds)
		})

		t.Run("Add key and replace time that has passed", func(t *testing.T) {
			t.Parallel()
			schedule := NewScheduler()
			err := schedule.AddDate("add#1", time.Now().UTC().Add(24*time.Hour), fn)
			assert.Nil(t, err)
			isExists, ds := schedule.read("add#1")
			assert.Equal(t, isExists, true)
			assert.NotNil(t, ds)
			err = schedule.ReplaceDateTime("add#1", time.Now().UTC().Add(-24*time.Hour), fn)
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrDateTimeLessThanNow)
		})
	})
}

func TestListConverter(t *testing.T) {
	t.Run("List Converter Default", func(t *testing.T) {
		t.Parallel()

		var wg sync.WaitGroup
		schedule := NewScheduler()
		wg.Add(1000)
		for i := 1; i <= 1000; i++ {
			go func(index int) {
				defer wg.Done()
				key := fmt.Sprintf("add#%d", index)
				duration := time.Duration(index) * time.Minute
				err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
				assert.Nil(t, err)
			}(i)
		}

		wg.Wait()

		buf := &bytes.Buffer{}
		n, err := schedule.List(buf)

		responseSchedulers := schedule.toResponseScheduler()
		tw := table.NewWriter()
		tw.AppendHeader(table.Row{"No.", "Key", "Date Time"})
		for i := 0; i < len(responseSchedulers); i++ {
			tw.AppendRow(table.Row{i + 1, responseSchedulers[i].Key, responseSchedulers[i].Time})
		}
		actualResponse := []byte(tw.Render())

		assert.Nil(t, err)
		assert.Condition(t, func() bool {
			return n > 0
		})
		assert.Equal(t, buf.Bytes(), actualResponse)
	})

	t.Run("List Converter JSON", func(t *testing.T) {
		t.Parallel()

		var wg sync.WaitGroup
		schedule := NewScheduler()
		wg.Add(1000)
		for i := 1; i <= 1000; i++ {
			go func(index int) {
				defer wg.Done()
				key := fmt.Sprintf("add#%d", index)
				duration := time.Duration(index) * time.Minute
				err := schedule.AddDate(key, time.Now().UTC().Add(duration), fn)
				assert.Nil(t, err)
			}(i)
		}

		wg.Wait()
		buf := &bytes.Buffer{}
		n, err := schedule.List(buf, NewJsonResponse())
		assert.Nil(t, err)
		assert.Condition(t, func() bool {
			return n > 0
		})

		responseSchedulers := schedule.toResponseScheduler()
		structSchedulers := make([]jsonData, 0)
		for i := 0; i < len(responseSchedulers); i++ {
			structSchedulers = append(structSchedulers, jsonData{
				Key:      responseSchedulers[i].Key,
				DateTime: responseSchedulers[i].Time,
			})
		}

		actualOutput, err := json.Marshal(structSchedulers)
		assert.Nil(t, err)
		assert.Equal(t, buf.Bytes(), actualOutput)
	})
}
