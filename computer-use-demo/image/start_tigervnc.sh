#!/bin/sh
set -eu

rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}"

exec Xtigervnc ":${DISPLAY_NUM}" \
  -rfbport 5900 \
  -geometry "${WIDTH}x${HEIGHT}" \
  -depth 24 \
  -SecurityTypes None \
  -localhost no \
  -AlwaysShared
