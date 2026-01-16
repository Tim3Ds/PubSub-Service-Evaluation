# Makefile for building C++ gRPC examples with dependencies from source

BUILD_DIR = $(CURDIR)/build
DEPS_DIR = $(BUILD_DIR)/deps
PROTOBUF_VERSION = 3.21.12
GRPC_VERSION = v1.54.0

.PHONY: all clean deps protobuf grpc build

all: build

# Build protobuf from source
protobuf:
	@if [ -f "$(DEPS_DIR)/lib/libprotobuf.a" ] || [ -f "$(DEPS_DIR)/lib64/libprotobuf.a" ]; then \
		echo "protobuf already built in $(DEPS_DIR)"; \
	else \
		echo "Building protobuf $(PROTOBUF_VERSION) from source..."; \
		mkdir -p $(DEPS_DIR)/src; \
		cd $(DEPS_DIR)/src && \
		if [ ! -d "protobuf" ]; then \
			git clone --depth 1 --branch v$(PROTOBUF_VERSION) https://github.com/protocolbuffers/protobuf.git; \
		fi && \
		cd protobuf && \
		git submodule update --init --recursive && \
		cmake -B build \
			-DCMAKE_INSTALL_PREFIX=$(DEPS_DIR) \
			-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
			-Dprotobuf_BUILD_TESTS=OFF \
			-Dprotobuf_BUILD_SHARED_LIBS=OFF && \
		cmake --build build -j$$(nproc) && \
		cmake --install build; \
	fi

# Build gRPC from source (includes protobuf)
grpc: protobuf
	@if [ -f "$(DEPS_DIR)/lib/libgrpc++.a" ] || [ -f "$(DEPS_DIR)/lib64/libgrpc++.a" ]; then \
		echo "gRPC already built in $(DEPS_DIR)"; \
	else \
		echo "Building gRPC $(GRPC_VERSION) from source..."; \
		mkdir -p $(DEPS_DIR)/src; \
		cd $(DEPS_DIR)/src && \
		if [ ! -d "grpc" ]; then \
			git clone --depth 1 --branch $(GRPC_VERSION) https://github.com/grpc/grpc.git; \
		fi && \
		cd grpc && \
		git submodule update --init --recursive && \
		cmake -B build \
			-DCMAKE_INSTALL_PREFIX=$(DEPS_DIR) \
			-DCMAKE_PREFIX_PATH=$(DEPS_DIR) \
			-DgRPC_INSTALL=ON \
			-DgRPC_BUILD_TESTS=OFF \
			-DgRPC_PROTOBUF_PROVIDER=package \
			-DgRPC_SSL_PROVIDER=package && \
		cmake --build build -j$$(nproc) && \
		cmake --install build; \
	fi

deps: grpc

# Build the project using locally built dependencies
build: deps
	cmake -S . -B $(BUILD_DIR) \
		-DCMAKE_PREFIX_PATH=$(DEPS_DIR) \
		-DCMAKE_RUNTIME_OUTPUT_DIRECTORY=$(BUILD_DIR)/bin
	cmake --build $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)

clean-deps:
	rm -rf $(DEPS_DIR)
