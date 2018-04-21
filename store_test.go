// MIT License

// Copyright (c) 2018 Li Zebang

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Revision History:
//     Initial: 2018/04/20        Li Zebang

package store

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

var sum = 100000

func TestStoreNoDone(t *testing.T) {
	s := NewStore("test.db", "test")

	err := s.StartStore()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	for i := 0; i < sum; i++ {
		err := s.SendSignal(&Signal{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(200 * i),
			Done:  false,
		})
		if err != nil {
			panic(err)
		}
	}
}

func TestStoreRepeatDone(t *testing.T) {
	s := NewStore("test.db", "test")

	err := s.StartStore()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	for i := 0; i < sum; i++ {
		err := s.SendSignal(&Signal{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(200 * i),
			Done:  false,
		})
		if err != nil {
			panic(err)
		}
	}
	for i := 0; i < sum; i++ {
		err := s.SendSignal(&Signal{
			Key:   strconv.Itoa(i + sum),
			Value: strconv.Itoa(200 * (i + sum)),
			Done:  true,
		})
		if err != nil && err != ErrSignalChClosed {
			panic(err)
		}
	}
}

func TestStoreAllRight(t *testing.T) {
	s := NewStore("test.db", "test")

	err := s.StartStore()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	for i := 0; i < sum; i++ {
		err := s.SendSignal(&Signal{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(200 * i),
			Done:  false,
		})
		if err != nil {
			panic(err)
		}
	}
	err = s.SendSignal(&Signal{
		Key:   strconv.Itoa(sum),
		Value: strconv.Itoa(200 * sum),
		Done:  true,
	})
	if err != nil {
		panic(err)
	}
}

func TestStoreConcurrency(t *testing.T) {
	s := NewStore("test.db", "test")

	err := s.StartStore()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	wg := &sync.WaitGroup{}
	wg.Add(sum)
	for i := 0; i < sum; i++ {
		go func(j int) {
			defer wg.Done()
			err := s.SendSignal(&Signal{
				Key:   strconv.Itoa(j),
				Value: strconv.Itoa(200 * j),
				Done:  false,
			})
			if err != nil {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
	err = s.SendSignal(&Signal{
		Key:   strconv.Itoa(sum),
		Value: strconv.Itoa(200 * sum),
		Done:  true,
	})
	if err != nil {
		panic(err)
	}
}

func TestStoreSuddenlyQuit(t *testing.T) {
	s := NewStore("test.db", "test")

	err := s.StartStore()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	wg := &sync.WaitGroup{}
	wg.Add(sum)
	for i := 0; i < sum; i++ {
		go func(j int) {
			defer wg.Done()
			err := s.SendSignal(&Signal{
				Key:   strconv.Itoa(j),
				Value: strconv.Itoa(200 * j),
				Done:  false,
			})
			if err != nil {
				panic(err)
			}
			fmt.Println("sent:", j)
		}(i)
	}
	time.Sleep(1e2)
	panic("1111")
	wg.Wait()
	err = s.SendSignal(&Signal{
		Key:   strconv.Itoa(sum),
		Value: strconv.Itoa(200 * sum),
		Done:  true,
	})
	if err != nil {
		panic(err)
	}
}
