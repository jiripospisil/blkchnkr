run_test() {
  local test_fn=$1
  shift

  echo "${test_fn} $@ START $(date):"
  $test_fn "$@"
  echo "${test_fn} $@ END:  $(date):"
}

random_dev_id() {
  echo $(shuf -i 0-1048576 -n 1)
}

create_tmp_dir() {
  echo $(mktemp -d "/tmp/blkchnkr.XXXXXXXXXXX")
}

start_server() {
  local repo=$1

  ../target/debug/blkchnkr start -r "${repo}" &
  local pid=$!

  sleep 0.2

  if ! kill -0 $pid >/dev/null 2>&1; then
    echo "failed to launch the server"
    exit 1
  fi
}
