#!/bin/sh

version=$1

REPO_PATH="https://dl.bintray.com/jamespedwards42/libs/com/fabahaba/jedipus/$version/"
jar="jedipus-$version.jar"
detachedSig="$jar.asc"

wget "$REPO_PATH$detachedSig"
wget "$REPO_PATH$jar"

// Latest key can always be found @ https://keybase.io/jamespedwards42/key.asc
gpg --keyserver pgpkeys.mit.edu --recv-key 83D56348
gpg --verify "$detachedSig" "$jar"

rm "$detachedSig" "$jar"

exit 0;
