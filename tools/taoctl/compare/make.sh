#!/bin/bash

wd=`dirname $0`
GOBIN="$GOPATH/bin"
EXE=taoctl-compare
go build -o $EXE $wd
mv $EXE $GOBIN
