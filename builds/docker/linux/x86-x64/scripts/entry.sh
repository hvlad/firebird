#!/bin/sh
trap exit INT TERM
setarch $SET_ARCH /build.sh &
wait $!
