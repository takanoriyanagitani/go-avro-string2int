package lines2map

import (
	"bufio"
	"io"
	"iter"
	"log"
	"maps"
	"os"
)

func LinesToMap(
	lines iter.Seq2[string, error],
) map[string]int32 {
	var i iter.Seq2[string, int32] = func(
		yield func(string, int32) bool,
	) {
		var ix int32 = 0
		for line, e := range lines {
			if nil != e {
				log.Printf("error while creating map: %v\n", e)
				return
			}

			yield(line, ix)
			ix += 1
		}
	}
	return maps.Collect(i)
}

func ScannerToMap(
	lines *bufio.Scanner,
) map[string]int32 {
	var i iter.Seq2[string, error] = func(
		yield func(string, error) bool,
	) {
		for lines.Scan() {
			yield(lines.Text(), nil)
		}
	}
	return LinesToMap(i)
}

func ReaderToMap(
	lines io.Reader,
) map[string]int32 {
	return ScannerToMap(
		bufio.NewScanner(lines),
	)
}

func FileLikeToMap(
	stringsFile io.ReadCloser,
) map[string]int32 {
	defer stringsFile.Close()
	return ReaderToMap(stringsFile)
}

func FilenameToMap(
	stringsFilename string,
) map[string]int32 {
	f, e := os.Open(stringsFilename)
	if nil != e {
		log.Printf("%v: %s", e, stringsFilename)
		return map[string]int32{}
	}
	return FileLikeToMap(f)
}
