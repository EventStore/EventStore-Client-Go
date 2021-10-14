#!/bin/bash

set -e

PROTOBUF_VERSION="3.14.0"
UNAME_S=`uname -s`
OS=""
GENERATE_PROTOS=0

if [ $UNAME_S = "Linux" ]
then
    OS="linux-x86_64"
elif [ $UNAME_S = "Darwin" ]
then
    OS="osx-x86_64"
else
    echo "Unsupported operating system"
    exit 1
fi

PROTOC_TARBALL="protoc-$PROTOBUF_VERSION-$OS.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUF_VERSION/$PROTOC_TARBALL"

# Collect submitted flags.
for flag in "$@"
do
    if [ $flag = "--generate-protos" ]
    then
        GENERATE_PROTOS=1
    fi
done
#end

# Required tools
mkdir -p tools
pushd tools > /dev/null

if [ ! -d protobuf ]
then
    echo "Installing protoc $PROTOBUF_VERSION locally..."

    curl -LOs $PROTOC_URL
    unzip -qu $PROTOC_TARBALL -d protobuf
    rm $PROTOC_TARBALL

    echo "done."
fi

popd > /dev/null
# end

if [ $GENERATE_PROTOS -eq 1 ]
then
    go get google.golang.org/protobuf/cmd/protoc-gen-go
    go get google.golang.org/grpc/cmd/protoc-gen-go-grpc

    gopath=`go env GOPATH`

    for proto in $(find protos -name "*.proto")
    do
        bname=`basename $proto .proto`
        mkdir -p protos/$bname
        echo "Compiling $bname.proto ..."
        tools/protobuf/bin/protoc --proto_path=$PWD/protos --go_out=./protos/$(basename $proto .proto) --go-grpc_out=./protos/$(basename $proto .proto) --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --plugin=protoc-gen-go=$gopath/bin/protoc-gen-go --plugin=protoc-gen-go-grpc=$gopath/bin/protoc-gen-go-grpc $PWD/$proto
        echo "done."
    done

    echo "Protobuf code generation completed!"
fi

echo "Compiling project..."
go build -v ./esdb ./samples
echo "done."
