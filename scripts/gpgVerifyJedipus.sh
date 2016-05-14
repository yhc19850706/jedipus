#!/bin/sh

version=$1

REPO_PATH="https://dl.bintray.com/jamespedwards42/libs/com/fabahaba/jedipus/$version/"
jar="jedipus-$version.jar"
detachedSig="$jar.asc"

wget "$REPO_PATH$detachedSig"
wget "$REPO_PATH$jar"

gpg --keyserver pgpkeys.mit.edu --recv-key B7BF1143
gpg --verify "$detachedSig" "$jar"

rm "$detachedSig" "$jar"

exit 0;
