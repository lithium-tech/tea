#!/usr/bin/env bash
# Build and install Greenplum 6 into $HOME/local/gpdb.
#
# Usage: build-gpdb.sh <repo-url> <ref> [open-gpdb]
#
# Passing "open-gpdb" as the third argument enables the fixups needed for the
# open-gpdb fork (see the inline comments below); arenadata/gpdb builds without
# them.
set -eo pipefail

repo="$1"
ref="$2"
mode="${3:-}"

git clone "$repo" -b "$ref" --depth 1 gpdb
cd gpdb
git submodule update --init

configure_extra=()
if [ "$mode" = "open-gpdb" ]; then
  # ORCA in open-gpdb still builds with -std=c++98/gnu++98, but the system
  # Xerces-C 3.2 (Ubuntu 22.04) requires C++11 (char16_t); bump the ORCA C++
  # standard to gnu++14 (gnu++17 would reject ORCA's deprecated throw() specs).
  # gnu++14 + -Wextra then surfaces -Wdeprecated-copy / -Wnonnull-compare which
  # ORCA's -Werror turns fatal, so append -Wno-error=... last (wins over it).
  for mk in src/backend/gpopt/gpopt.mk \
            src/backend/gporca/gporca.mk \
            src/backend/gporca/libgpos/src/common/Makefile; do
    sed -i 's/-std=gnu++98/-std=gnu++14/g; s/-std=c++98/-std=gnu++14/g' "$mk"
    printf '\noverride CPPFLAGS := $(CPPFLAGS) -Wno-error=deprecated-copy -Wno-error=nonnull-compare\n' >> "$mk"
  done
  # open-gpdb defaults --with-mdblocales=yes and hard-requires the mdblocales
  # lib/header, which is not available on the CI image.
  configure_extra+=(--without-mdblocales)
fi

# TODO(gmusya): consider using --enable-cassert
./configure --with-perl --with-python --with-libxml --with-gssapi \
  --with-pythonsrc-ext "${configure_extra[@]}" --prefix="$HOME/local/gpdb"

if [ "$mode" = "open-gpdb" ]; then
  # src/common (FRONTEND) includes the backend-generated utils/errcodes.h but
  # has no order-only dep on it, so -j8 races ("errcodes.h: No such file").
  # Force the generated header first.
  make -C src/backend submake-errcodes
fi

make -j8
make -j8 install

cd ..
rm -rf gpdb
