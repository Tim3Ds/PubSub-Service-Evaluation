# Makefile for Redis 6.2.6 server setup

REDIS_VERSION = 6.2.6
REDIS_URL = https://download.redis.io/releases/redis-$(REDIS_VERSION).tar.gz
BUILD_DIR = build
SERVER_DIR = $(BUILD_DIR)/redis-$(REDIS_VERSION)
TARBALL = $(BUILD_DIR)/redis-$(REDIS_VERSION).tar.gz

.PHONY: all download build install clean build-cpp

all: download build install

download:
	mkdir -p $(BUILD_DIR)
	@if [ ! -f $(TARBALL) ]; then \
		echo "Downloading Redis v$(REDIS_VERSION)..."; \
		curl -L $(REDIS_URL) -o $(TARBALL); \
	else \
		echo "Redis tarball already exists."; \
	fi

build:
	@if [ ! -d $(SERVER_DIR) ]; then \
		echo "Extracting Redis..."; \
		tar -xzf $(TARBALL) -C $(BUILD_DIR); \
	fi
	@echo "Building Redis..."
	$(MAKE) -C $(SERVER_DIR)

install:
	@echo "Redis server is built in $(SERVER_DIR)/src"
	@echo "To start: $(SERVER_DIR)/src/redis-server"

build-cpp:
	$(MAKE) -C cpp build

clean:
	rm -rf $(BUILD_DIR)
	$(MAKE) -C cpp clean
