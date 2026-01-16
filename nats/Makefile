# Makefile for downloading and building nats-server


NATS_VERSION ?= latest
NATS_REPO = https://github.com/nats-io/nats-server.git
NATS_DIR = nats-server
BUILD_DIR = build/bin
INSTALL_BIN = $(BUILD_DIR)/nats-server


.PHONY: all download build install clean

all: download build install

# Clone or update the repository
download:
	@if [ ! -d $(NATS_DIR) ]; then \
		git clone $(NATS_REPO) $(NATS_DIR); \
	else \
		cd $(NATS_DIR) && git fetch --all && git pull; \
	fi
	@if [ "$(NATS_VERSION)" != "latest" ]; then \
		cd $(NATS_DIR) && git checkout $(NATS_VERSION); \
	fi


# Build the nats-server binary
build:
	mkdir -p $(BUILD_DIR)
	cd $(NATS_DIR) && go build -o ../$(INSTALL_BIN) -v


# Install the built binary to build/bin
install:
	@echo "nats-server binary is in $(INSTALL_BIN)"

# Remove the cloned repo and build artifacts
clean:
	rm -rf $(NATS_DIR) $(BUILD_DIR)
