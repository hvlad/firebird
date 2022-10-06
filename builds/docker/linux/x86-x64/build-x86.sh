#!/bin/sh
docker build \
	--build-arg ARG_BASE=i386/ubuntu:18.04 \
	--build-arg ARG_SET_ARCH=i686 \
	--build-arg ARG_TARGET_ARCH=i586-pc-linux-gnu \
	--build-arg ARG_CTNF_CONFIG=crosstool-ng-config-x86 \
	-t asfernandes/firebird-builder:fb5-x86-ng-v1 .
