# Makefile for building C++ NATS examples with CMake

BUILD_DIR = build

.PHONY: all clean

all:
	cmake -S . -B $(BUILD_DIR)
	cmake --build $(BUILD_DIR)

# Provide a `build` alias for consistency with other service Makefiles
.PHONY: build
build: all

clean:
	rm -rf $(BUILD_DIR)
