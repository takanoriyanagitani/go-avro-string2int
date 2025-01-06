#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/output.avsc
export ENV_STR2INT_LINES_NAME=./sample.d/conv.txt
export ENV_STR2INT_TARGET_NAME=name

cat sample.d/sample.avro |
	./avro-str2serial |
	rq \
		--input-avro \
		--output-json |
	jq -c
