#!/bin/sh
set -eu

exec supervisord --nodaemon --configuration /home/computeruse/supervisord.conf
