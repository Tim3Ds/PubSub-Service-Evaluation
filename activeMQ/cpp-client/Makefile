# Variables
ACTIVEMQCPP_VERSION=3.9.5
ACTIVEMQCPP_TARBALL=activemq-cpp-library-$(ACTIVEMQCPP_VERSION)-src.tar.gz
ACTIVEMQCPP_URL=https://archive.apache.org/dist/activemq/activemq-cpp/$(ACTIVEMQCPP_VERSION)/$(ACTIVEMQCPP_TARBALL)
ACTIVEMQCPP_SRC=activemq-cpp-library-$(ACTIVEMQCPP_VERSION)
ACTIVEMQCPP_INSTALL=$(PWD)/build

activemq-cpp := $(ACTIVEMQCPP_INSTALL)/lib/libactivemq-cpp.so

.PHONY: build clean

build: $(activemq-cpp)
	cmake -B build -S . -DCMAKE_PREFIX_PATH=$(ACTIVEMQCPP_INSTALL)
	cmake --build build

# Download, unpack, and build the ActiveMQ-CPP library if not already built
$(activemq-cpp):
	@if [ ! -d "$(ACTIVEMQCPP_SRC)" ]; then \
	  echo "Downloading $(ACTIVEMQCPP_TARBALL)..."; \
	  wget -c $(ACTIVEMQCPP_URL); \
	  tar -xf $(ACTIVEMQCPP_TARBALL); \
	fi
	@if [ ! -f "$(ACTIVEMQCPP_INSTALL)/lib/libactivemq-cpp.so" ]; then \
	  echo "Building ActiveMQ-CPP (autotools)..."; \
	  (cd $(ACTIVEMQCPP_SRC) && ./configure --prefix=$(ACTIVEMQCPP_INSTALL)); \
	  $(MAKE) -C $(ACTIVEMQCPP_SRC) -j$(shell nproc); \
	  $(MAKE) -C $(ACTIVEMQCPP_SRC) install -j$(shell nproc); \
	fi

clean:
	rm -rf build
	rm -rf $(ACTIVEMQCPP_SRC)
	rm -f $(ACTIVEMQCPP_TARBALL)