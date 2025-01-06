package str2int

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log"
)

var (
	ErrInvalidTarget error = errors.New("expected string")
	ErrUnableToMap   error = errors.New("unable to map")
)

type StringToInt func(string) (int32, error)

type StringToIntMap map[string]int32

func MapError(rejected string) error {
	return fmt.Errorf("%w: %s", ErrUnableToMap, rejected)
}

func (m StringToIntMap) ToStringToInt(
	onMissing func(string) error,
) StringToInt {
	return func(s string) (int32, error) {
		i, found := m[s]
		switch found {
		case true:
			return i, nil
		default:
			log.Printf("map: %v\n", m)
			return 0, onMissing(s)
		}
	}
}

func (m StringToIntMap) ToStringToIntDefault() StringToInt {
	return m.ToStringToInt(MapError)
}

func (s StringToInt) ToNullable(val string) (sql.Null[int32], error) {
	converted, e := s(val)
	ret := sql.Null[int32]{
		V:     converted,
		Valid: nil == e,
	}
	return ret, e
}

func (s StringToInt) Convert(original sql.Null[string]) (sql.Null[int32], error) {
	switch original.Valid {
	case false:
		return sql.Null[int32]{}, nil
	default:
		return s.ToNullable(original.V)
	}
}

func (s StringToInt) ConvertToPtr(original sql.Null[string]) (*int32, error) {
	nullable, e := s.Convert(original)
	if nil != e {
		return nil, e
	}

	switch nullable.Valid {
	case true:
		return &nullable.V, nil
	default:
		return nil, nil
	}
}

func (s StringToInt) MapsToMaps(
	original iter.Seq2[map[string]any, error],
	targetColumnName string,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		for row, e := range original {
			clear(buf)

			if nil != e {
				yield(buf, e)
				return
			}

			for key, val := range row {
				if targetColumnName != key {
					buf[key] = val
				}
			}

			var target any = row[targetColumnName]
			var tgt sql.Null[string]
			switch t := target.(type) {
			case string:
				tgt.Valid = true
				tgt.V = t
			case nil:
			default:
				yield(buf, ErrInvalidTarget)
				return
			}

			mapd, e := s.ConvertToPtr(tgt)
			if nil != e {
				yield(buf, e)
				return
			}

			buf[targetColumnName] = mapd

			if !yield(buf, nil) {
				return
			}
		}
	}
}

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct {
	BlobSizeMax int
}

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
