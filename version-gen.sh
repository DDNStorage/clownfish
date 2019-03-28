#!/bin/sh

VERSION="1.4"
if [ -d .git ]; then
	VERSION=${VERSION}".g$(git rev-parse --short HEAD)"
fi

printf "%s" "$VERSION"
