BUILD_DIR = $(shell pwd)/build
BIN_DIR = $(BUILD_DIR)/bin
PREFIX = $(abspath $(BUILD_DIR))
HIREDIS_LIB = $(PREFIX)/lib/libhiredis.a
HIREDIS_INC = $(PREFIX)/include

.PHONY: hiredis build clean

DEFAULT : build

hiredis:
	@if [ -f "$(HIREDIS_LIB)" ]; then \
		echo "hiredis already built in $(PREFIX)"; \
	else \
		mkdir -p $(BUILD_DIR)/_deps; \
		git clone --depth 1 --branch v1.2.0 https://github.com/redis/hiredis.git $(BUILD_DIR)/_deps/hiredis-src; \
		$(MAKE) -C $(BUILD_DIR)/_deps/hiredis-src; \
		mkdir -p $(PREFIX)/lib $(PREFIX)/include/hiredis; \
		cp $(BUILD_DIR)/_deps/hiredis-src/libhiredis.a $(PREFIX)/lib/; \
		cp $(BUILD_DIR)/_deps/hiredis-src/*.h $(PREFIX)/include/hiredis/; \
	fi

build:
	$(MAKE) hiredis
	cmake -S . -B $(BUILD_DIR) -DCMAKE_PREFIX_PATH=$(PREFIX) -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=$(BIN_DIR)
	cmake --build $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)
