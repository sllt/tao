#!/bin/bash

wd=$(pwd)
output="$wd/hello"

rm -rf $output

taoctl rpc protoc -I $wd "$wd/hello.proto" --go_out="$output/pb" --go-grpc_out="$output/pb" --zrpc_out="$output" --multiple

if [ $? -ne 0 ]; then
    echo "Generate failed"
    exit 1
fi

GOPROXY="https://goproxy.cn,direct" && go mod tidy

if [ $? -ne 0 ]; then
    echo "Tidy failed"
    exit 1
fi

go test ./...