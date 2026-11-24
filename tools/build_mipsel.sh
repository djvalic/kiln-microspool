#!/bin/sh
# Builds bin/microspool for Kiln OS (MIPS32 little-endian, glibc) with
# Kiln OS's own Buildroot toolchain, the same compiler as the image.
#
#   CC=/path/to/buildroot/output/host/bin/mipsel-buildroot-linux-gnu-gcc \
#       tools/build_mipsel.sh
#
# On Kiln's builder: CC=/work/repo/buildroot/output/host/bin/mipsel-buildroot-linux-gnu-gcc
set -eu
cd "$(dirname "$0")/.."
: "${CC:?set CC to the Kiln OS Buildroot mipsel gcc}"
VERSION=$(sed -n 's/^VERSION *?= *//p' Makefile)
KILN=$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' plugin.json)
mkdir -p build bin
python3 tools/embed_ui.py ui/index.html build/ui_gz.h
"$CC" -std=c11 -Wall -Wextra -O2 -Ivendor -Ibuild -I. \
	-D_TIME_BITS=64 -D_FILE_OFFSET_BITS=64 \
	-DMICROSPOOL_VERSION="\"$VERSION-kiln-$KILN\"" \
	-ffunction-sections -fdata-sections \
	-o bin/microspool microspool.c vendor/cJSON.c \
	-Wl,--gc-sections -s -lm
{
	echo "bin/microspool: microspool $VERSION (kiln-microspool $KILN)"
	# SOURCE_COMMIT: for a build from an exported tree (git archive)
	echo "source: this repository at ${SOURCE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo unknown)$(git diff --quiet HEAD 2>/dev/null || echo ' (with local changes)')}"
	echo "compiler: $("$CC" --version | head -1)"
	echo "sha256: $(sha256sum bin/microspool | cut -d' ' -f1)"
} > bin/SOURCES
cat bin/SOURCES
