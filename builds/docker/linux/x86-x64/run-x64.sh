#!/bin/sh
docker run --platform amd64 --rm --user `id -u`:`id -g` -v `pwd`/../../../..:/firebird -t asfernandes/firebird-builder:fb5-x64-ng-v1
