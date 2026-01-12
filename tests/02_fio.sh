#!/usr/bin/bash

set -ue

cd "$(dirname "${BASH_SOURCE[0]}")"
. ./common.sh

test_02_fio() (
  local dev_id=$(random_dev_id)
  local tmp_dir=$(create_tmp_dir)

  ../target/debug/blkchnkr init --dev-id "${dev_id}" -r "${tmp_dir}/repo" \
    --size 500M --chunk-size 32M

  start_server "${tmp_dir}/repo"
  local pid=$!

  for variant in rw randrw; do
    fio --name "02_fio_${variant}" --filename="/dev/ublkb${dev_id}" --rw=${variant} \
      --bsrange=4k-4M --direct=1 --ioengine=libaio --iodepth=128 --verify=crc32c \
      --verify_state_save=0 --verify_fatal 1 --time_based --runtime=30s
  done

  kill ${pid}
  rm -rf "${tmp_dir}"
)

run_test test_02_fio
