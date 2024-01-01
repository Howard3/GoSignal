package serde

import (
	"encoding/json"

	"github.com/Howard3/gosignal"
)

type JSON[T any] struct{}

func (j JSON[T]) Serialize(m gosignal.Message[T]) ([]byte, error) {
	return json.Marshal(m)
}

func (j JSON[T]) Deserialize(b []byte) (gosignal.Message[T], error) {
	var m gosignal.Message[T]
	err := json.Unmarshal(b, &m)
	return m, err
}
