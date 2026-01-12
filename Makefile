bindings:
	bindgen --ignore-methods --with-derive-default --wrap-unsafe-ops /usr/include/ublk_cmd.h > src/bindings.rs

.PHONY: bindings
