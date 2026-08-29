#!/bin/sh
set -eu

until nc -z 127.0.0.1 5900; do sleep 0.1; done
exec websockify --web=/usr/share/novnc 6080 127.0.0.1:5900
