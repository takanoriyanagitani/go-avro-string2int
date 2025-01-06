package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	si "github.com/takanoriyanagitani/go-avro-string2int"
	. "github.com/takanoriyanagitani/go-avro-string2int/util"

	lm "github.com/takanoriyanagitani/go-avro-string2int/convmap/lines2map"

	dh "github.com/takanoriyanagitani/go-avro-string2int/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-string2int/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var string2intLinesFilename IO[string] = EnvValByKey("ENV_STR2INT_LINES_NAME")

var str2intMap IO[map[string]int32] = Bind(
	string2intLinesFilename,
	Lift(func(filename string) (map[string]int32, error) {
		return lm.FilenameToMap(filename), nil
	}),
)

var str2int IO[si.StringToInt] = Bind(
	str2intMap,
	Lift(func(m map[string]int32) (si.StringToInt, error) {
		return si.StringToIntMap(m).ToStringToIntDefault(), nil
	}),
)

var targetColName IO[string] = EnvValByKey("ENV_STR2INT_TARGET_NAME")

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

type Config struct {
	si.StringToInt
	TargetColumnName string
}

var config IO[Config] = Bind(
	str2int,
	func(conv si.StringToInt) IO[Config] {
		return Bind(
			targetColName,
			Lift(func(col string) (Config, error) {
				return Config{
					StringToInt:      conv,
					TargetColumnName: col,
				}, nil
			}),
		)
	},
)

func (c Config) MapsToMaps(
	m iter.Seq2[map[string]any, error],
) iter.Seq2[map[string]any, error] {
	return c.StringToInt.MapsToMaps(m, c.TargetColumnName)
}

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	stdin2maps,
	func(
		m iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			config,
			Lift(func(
				c Config,
			) (iter.Seq2[map[string]any, error], error) {
				return c.MapsToMaps(m), nil
			}),
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
