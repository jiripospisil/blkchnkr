#!/usr/bin/bash

set -ue

cd "$(dirname "${BASH_SOURCE[0]}")"
. ./common.sh

# Checks that a potentially crashed server can be recovered.
test_04_recovery() (
  local dev_id=$(random_dev_id)
  local tmp_dir=$(create_tmp_dir)

  ../target/debug/blkchnkr init --dev-id "${dev_id}" -r "${tmp_dir}/repo" \
    --size 1G --chunk-size 64M

  start_server "${tmp_dir}/repo"
  local pid=$!

  # Create a random file.
  head -c 450M /dev/random > "${tmp_dir}/random_file"

  # Compute the random file's sha.
  local sha=$(sha256sum "${tmp_dir}/random_file" | cut -d ' ' -f 1)

  # Create a file system
  mkfs.xfs -q "/dev/ublkb${dev_id}"

  # Mount the file system
  mount --mkdir "/dev/ublkb${dev_id}" "${tmp_dir}/mount"

  # Copy over the file. Use dd to force large requests.
  dd if="${tmp_dir}/random_file" of="${tmp_dir}/mount/random_file" bs=4M

  # Drop caches
  sync
  echo 3 > "/proc/sys/vm/drop_caches"

  # Forcefully kill the server
  kill -9 ${pid}
  sleep 2

  # Do recovery
  start_server "${tmp_dir}/repo"
  local pid=$!

  # Make sure the mount still works
  local sha_actual=$(sha256sum "${tmp_dir}/mount/random_file" | cut -d ' ' -f 1)

  if [[ "${sha}" != "${sha_actual}" ]]; then
    echo "shas don't match"
    exit 1
  fi

  # Clean up
  umount -l "${tmp_dir}/mount"
  kill ${pid}
  rm -rf "${tmp_dir}"
)

run_test test_04_recovery
