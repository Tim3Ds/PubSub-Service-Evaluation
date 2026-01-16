BUILD_DIR = $(shell pwd)/build
BIN_DIR = $(BUILD_DIR)/bin
PREFIX = $(abspath $(BUILD_DIR))
LIBRABBITMQ_LIB = $(PREFIX)/lib/librabbitmq.a
LIBRABBITMQ_SO = $(PREFIX)/lib/librabbitmq.so
LIBRABBITMQ_INC = $(PREFIX)/include

.PHONY: librabbitmq build clean

DEFAULT : build

# Clone, build and install rabbitmq-c into $(BUILD_DIR) if not already present
librabbitmq:
	@if [ -f "$(LIBRABBITMQ_LIB)" ] || [ -f "$(LIBRABBITMQ_SO)" ]; then \
		echo "rabbitmq-c already built in $(PREFIX)"; \
	else \
		mkdir -p $(BUILD_DIR)/_deps/rabbitmq-c-build; \
		git clone --depth 1 --branch v0.15.0 https://github.com/alanxz/rabbitmq-c.git $(BUILD_DIR)/_deps/rabbitmq-c-src; \
		cd $(BUILD_DIR)/_deps/rabbitmq-c-build && \
		cmake -S ../../_deps/rabbitmq-c-src -B . -DCMAKE_INSTALL_PREFIX=$(PREFIX) -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_TOOLS=OFF && \
		cmake --build . --parallel && \
		cmake --install . --config Release --prefix $(PREFIX); \
	fi

build:
	$(MAKE) librabbitmq
	cmake -S . -B $(BUILD_DIR) -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_PREFIX_PATH=$(PREFIX) -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=$(BIN_DIR)
	cmake --build $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)
