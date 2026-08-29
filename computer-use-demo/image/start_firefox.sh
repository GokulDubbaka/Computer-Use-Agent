#!/bin/sh
set -eu

until xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; do sleep 0.1; done
until pgrep -x openbox >/dev/null 2>&1; do sleep 0.1; done

exec firefox-esr \
  --no-remote \
  --new-instance \
  --width "${WIDTH}" \
  --height "${HEIGHT}" \
  about:blank
