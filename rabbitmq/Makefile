# Makefile for downloading and setting up RabbitMQ server (binary distribution)

RABBITMQ_VERSION ?= 3.7.28
RABBITMQ_URL = https://github.com/rabbitmq/rabbitmq-server/releases/download/v$(RABBITMQ_VERSION)/rabbitmq-server-generic-unix-$(RABBITMQ_VERSION).tar.xz
BUILD_DIR = build
INSTALL_DIR = $(BUILD_DIR)/rabbitmq_server-$(RABBITMQ_VERSION)
TARBALL = $(BUILD_DIR)/rabbitmq-server-generic-unix-$(RABBITMQ_VERSION).tar.xz

.PHONY: all download build install clean

all: download install

# Create build directory and download the binary distribution
download:
	mkdir -p $(BUILD_DIR)
	@if [ ! -f $(TARBALL) ]; then \
		echo "Downloading RabbitMQ v$(RABBITMQ_VERSION)..."; \
		curl -L $(RABBITMQ_URL) -o $(TARBALL); \
	else \
		echo "RabbitMQ tarball already exists."; \
	fi

# Extract the binary distribution
install:
	@if [ ! -d $(INSTALL_DIR) ]; then \
		echo "Extracting RabbitMQ..."; \
		tar -xJf $(TARBALL) -C $(BUILD_DIR); \
	else \
		echo "RabbitMQ already extracted in $(INSTALL_DIR)"; \
	fi
	@echo "RabbitMQ server is in $(INSTALL_DIR)"
	@echo "To start: $(INSTALL_DIR)/sbin/rabbitmq-server"

# Run the C++ build
build-cpp:
	$(MAKE) -C cpp build

# Clean up build artifacts
clean:
	rm -rf $(BUILD_DIR)
	$(MAKE) -C cpp clean
