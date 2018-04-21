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
	"errors"
	"fmt"
	"sync"
)

type Store struct {
	path           string
	bucket         string
	signalCh       chan *Signal
	singalChStatus bool
	done           chan struct{}
	exit           chan struct{}
	once           sync.Once
}

type Signal struct {
	Key   string
	Value string
	Done  bool
}

var (
	ErrSignalChClosed = errors.New("singal channel is closed")
)

func NewStore(path, bucket string) *Store {
	return &Store{
		path:     path,
		bucket:   bucket,
		signalCh: make(chan *Signal),
		done:     make(chan struct{}),
		exit:     make(chan struct{}),
	}
}

func (s *Store) StartStore() (err error) {
	db, err := open(s.path)
	if err != nil {
		return err
	}

	go func() {
		wg := &sync.WaitGroup{}
		defer func() {
			wg.Wait()
			close(s.signalCh)
			db.Close()
			close(s.exit)
		}()
		for {
			select {
			case signal := <-s.signalCh:
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := set(db, []byte(s.bucket), []byte(signal.Key), []byte(signal.Value))
					if err != nil {
						panic(err)
					}
					fmt.Println(signal.Key)
					if signal.Done {
						go s.Close()
					}
				}()
			case <-s.done:
				return
			}
		}
	}()

	return nil
}

func (s *Store) SendSignal(signal *Signal) error {
	if s.singalChStatus {
		return ErrSignalChClosed
	}
	s.singalChStatus = signal.Done
	s.signalCh <- signal
	return nil
}

func (s *Store) Close() {
	s.once.Do(func() {
		close(s.done)
	})
	<-s.exit
}
