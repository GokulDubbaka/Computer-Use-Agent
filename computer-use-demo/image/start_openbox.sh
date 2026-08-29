#!/bin/sh
set -eu

until xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; do sleep 0.1; done
exec dbus-run-session -- openbox
