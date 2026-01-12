#!/usr/bin/bash

set -ue

cd "$(dirname "${BASH_SOURCE[0]}")"

./01_sha.sh
./02_fio.sh
./03_brtfs.sh
./04_recovery.sh

echo "PASS"
