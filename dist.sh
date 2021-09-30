#!/bin/bash

# 1. commit to bump the version (little_bigtable.go)
# 2. tag that commit
# 3. use dist.sh to produce tar.gz for all platforms
# 4. update the release metadata on github / upload the binaries

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

version=$(awk '/version / {print $NF}' < $DIR/little_bigtable.go | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... running tests"
./test.sh

mkdir -p dist
for target in "linux/amd64"; do
    os=${target%/*}
    arch=${target##*/}
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d ${TMPDIR:-/tmp}/build-XXXXX)
    TARGET="little_bigtable-$version.$os-$arch.$goversion"
    GOOS=$os GOARCH=$arch \
        go build --tags "$os" -o $BUILD/$TARGET .
    pushd $BUILD
    sudo chown -R 0:0 $TARGET
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    popd
    sudo rm -r $BUILD
done
