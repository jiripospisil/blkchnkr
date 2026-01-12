#!/usr/bin/bash

set -ue

cd "$(dirname "${BASH_SOURCE[0]}")"
. ./common.sh

# Creates a btrfs filesystem, puts some data into it and then runs scrub.
test_03_btrfs() (
  local chunk_size=$1
  local dev_id=$(random_dev_id)
  local tmp_dir=$(create_tmp_dir)

  ../target/debug/blkchnkr init --dev-id "${dev_id}" -r "${tmp_dir}/repo" \
    --size 1G --chunk-size 512M

  start_server "${tmp_dir}/repo"
  local pid=$!

  # Create a file system
  mkfs.btrfs "/dev/ublkb${dev_id}"

  # Mount the file system
  mount --mkdir "/dev/ublkb${dev_id}" "${tmp_dir}/mount"

  # Create random files.
  head -c 450M /dev/random > "${tmp_dir}/mount/random_file"
  head -c 150M /dev/random > "${tmp_dir}/mount/random_file2"
  head -c 150M /dev/random > "${tmp_dir}/mount/random_file3"
  head -c 50M /dev/random > "${tmp_dir}/mount/random_file4"
  head -c 50M /dev/random > "${tmp_dir}/mount/random_file5"

  # Umount and check the fs.
  umount -l "${tmp_dir}/mount"
  btrfs check "/dev/ublkb${dev_id}"

  # Run scrub
  mount --mkdir "/dev/ublkb${dev_id}" "${tmp_dir}/mount"
  btrfs scrub start -B "/dev/ublkb${dev_id}"

  # Clean up
  umount -l "${tmp_dir}/mount"
  kill ${pid}
  rm -rf "${tmp_dir}"
)

run_test test_03_btrfs 16M
run_test test_03_btrfs 32M
run_test test_03_btrfs 64M
run_test test_03_btrfs 128M
run_test test_03_btrfs 256M
run_test test_03_btrfs 512M
run_test test_03_btrfs 1024M
