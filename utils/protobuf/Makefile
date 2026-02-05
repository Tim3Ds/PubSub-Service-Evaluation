# Makefile for building protobuf library
# This provides a centralized protobuf installation for all C++ services

BUILD_DIR = $(CURDIR)/build
PROTOBUF_VERSION = 3.21.12

.PHONY: all clean protobuf

all: protobuf

# Build protobuf from source
protobuf:
	@if [ -f "$(BUILD_DIR)/lib/libprotobuf.a" ] || [ -f "$(BUILD_DIR)/lib64/libprotobuf.a" ]; then \
		echo "protobuf already built in $(BUILD_DIR)"; \
	else \
		echo "Building protobuf $(PROTOBUF_VERSION) from source..."; \
		mkdir -p $(BUILD_DIR)/src; \
		cd $(BUILD_DIR)/src && \
		if [ ! -d "protobuf" ]; then \
			git clone --depth 1 --branch v$(PROTOBUF_VERSION) https://github.com/protocolbuffers/protobuf.git; \
		fi && \
		cd protobuf && \
		git submodule update --init --recursive && \
		cmake -B build \
			-DCMAKE_INSTALL_PREFIX=$(BUILD_DIR) \
			-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
			-Dprotobuf_BUILD_TESTS=OFF \
			-Dprotobuf_BUILD_SHARED_LIBS=OFF && \
		cmake --build build -j$$(nproc) && \
		cmake --install build; \
	fi

clean:
	rm -rf $(BUILD_DIR)

# For testing - show where protobuf is installed
info:
	@echo "Protobuf build directory: $(BUILD_DIR)"
	@echo "Include directory: $(BUILD_DIR)/include"
	@echo "Library directory: $(BUILD_DIR)/lib or $(BUILD_DIR)/lib64"
	@if [ -f "$(BUILD_DIR)/lib/libprotobuf.a" ] || [ -f "$(BUILD_DIR)/lib64/libprotobuf.a" ]; then \
		echo "Status: ✓ Protobuf is built"; \
	else \
		echo "Status: ✗ Protobuf not built (run 'make protobuf')"; \
	fi
