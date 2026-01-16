# ActiveMQ C++ client examples

This folder contains simple C++ publisher/listener examples using the ActiveMQ-CPP (CMS/OpenWire) library.

Prerequisites (AlmaLinux/RHEL/CentOS):

- A C++ toolchain and build tools:
  ```bash
  sudo dnf install -y gcc-c++ make cmake wget unzip
  ```
- APR and APR-util development headers:
  ```bash
  sudo dnf install -y apr-devel apr-util-devel
  ```
- OpenSSL development headers (if building with SSL support):
  ```bash
  sudo dnf install -y openssl-devel
  ```
- ActiveMQ-CPP (activemq-cpp) library. There may not be a packaged version in base repos; build and install from source:
  1. Download the ActiveMQ-CPP source from https://activemq.apache.org/cms
  2. Follow the build instructions in the ActiveMQ-CPP docs. Typical steps:
     ```bash
     # Example (adjust versions/paths):
     tar xzf activemq-cpp-x.y.z-src.tar.gz
     cd activemq-cpp-x.y.z
     mkdir build && cd build
     cmake ..
     make -j$(nproc)
     sudo make install
     ```
  3. After install, headers are commonly at `/usr/local/include/activemq-cpp-<version>` and libs in `/usr/local/lib`.

Build with CMake
----------------

From this folder:

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

If the `activemq-cpp` library is installed in a non-standard prefix, set `CMAKE_PREFIX_PATH` or `LD_LIBRARY_PATH` accordingly.

Run
---

Set environment variables (optional):

```bash
export ACTIVEMQ_HOST=localhost
export ACTIVEMQ_PORT=61616
export ACTIVEMQ_USER=admin
export ACTIVEMQ_PASSWORD=password
```

Start the listener in one terminal:

```bash
./listener
```

In another terminal run the publisher:

```bash
./publisher
```

If you need an AMQP-based C++ client instead, consider `qpid-proton-cpp` (Qpid Proton C++). I can add an AMQP example if you'd prefer that.


### Note
I need a generic test I can plug any of these [activeMQ,nats,zeroMQ]