# blkchnkr

Status: It compiled.

Blkchnkr is a ublk server which creates a block device backed by an on demand
allocated files.

```
$ blkchnkr init -r /tmp/repository --dev-id 42 --size 1t
[INFO]	Created a new repository at /tmp/repository

# blkchnkr start -r /tmp/repository
[INFO]	Starting up (v0.1.0)
[INFO]	Created a new block device at /dev/ublkb42
[INFO]	Ready!

# mkfs.xfs /dev/ublkb42

# mount --mkdir /dev/ublkb42 /tmp/mounted
```

## Building

```bash
$ cargo install --frozen --path .

# Development
$ cargo build --features debug
```

## License

GPL-3.0
