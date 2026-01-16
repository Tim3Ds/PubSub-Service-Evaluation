BUILD_DIR = $(shell pwd)/build
BIN_DIR = $(BUILD_DIR)/bin
PREFIX = $(abspath $(BUILD_DIR))
LIBZMQ_LIB = $(PREFIX)/lib64/libzmq.a
LIBZMQ_SO = $(PREFIX)/lib/libzmq.so
LIBZMQ_INC = $(PREFIX)/include

.PHONY: libzmq build clean

DEFAULT : build

# Clone, build and install libzmq into $(BUILD_DIR) if not already present
libzmq:
	@if [ -f "$(LIBZMQ_LIB)" ] || [ -f "$(LIBZMQ_SO)" ]; then \
		echo "libzmq already built in $(PREFIX)"; \
	else \
		mkdir -p $(BUILD_DIR)/_deps/libzmq-build; \
		git clone --depth 1 --branch v4.3.3 https://github.com/zeromq/libzmq.git $(BUILD_DIR)/_deps/libzmq-src; \
		cd $(BUILD_DIR)/_deps/libzmq-build && \
		cmake -S ../../_deps/libzmq-src -B . -DCMAKE_INSTALL_PREFIX=$(PREFIX) -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release && \
		cmake --build . --parallel && \
		cmake --install . --config Release --prefix $(PREFIX); \
		git clone --depth 1 --branch v4.11.0 https://github.com/zeromq/cppzmq.git $(BUILD_DIR)/_deps/cppzmq-src; \
		cp -r $(BUILD_DIR)/_deps/cppzmq-src/zmq*.hpp $(LIBZMQ_INC)/; \
	fi

build:
	$(MAKE) libzmq
	cmake -S . -B $(BUILD_DIR) -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_PREFIX_PATH=$(PREFIX) -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=$(BIN_DIR)
	cmake --build $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)
