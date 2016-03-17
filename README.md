# Wmibeat

Welcome to Wmibeat.

Ensure that this folder is at the following location:
`${GOPATH}/github.com/eskibars`

## Getting Started with Wmibeat

### Init Project
To get running with Wmibeat, run the following commands:

```
make init
```


To push Wmibeat in the git repository, run the following commands:

```
git commit 
git remote set-url origin https://github.com/eskibars/wmibeat
git push origin master
```

For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).

### Build

To build the binary for Wmibeat run the command below. This will generate a binary
in the same directory with the name wmibeat.

```
make
```


### Run

To run Wmibeat with debugging output enabled, run:

```
./wmibeat -c wmibeat.yml -e -d "*"
```


### Test

To test Wmibeat, run the following commands:

```
make testsuite
```

alternatively:
```
make unit-tests
make system-tests
make integration-tests
make coverage-report
```

The test coverage is reported in the folder `./build/coverage/`


### Package

To cross-compile and package Wmibeat for all supported platforms, run the following commands:

```
cd dev-tools/packer
make deps
make images
make
```

### Update

Each beat has a template for the mapping in elasticsearch and a documentation for the fields
which is automatically generated based on `etc/fields.yml`.
To generate etc/wmibeat.template.json and etc/wmibeat.asciidoc

```
make update
```


### Cleanup

To clean  Wmibeat source code, run the following commands:

```
make fmt
make simplify
```

To clean up the build directory and generated artifacts, run:

```
make clean
```


### Clone

To clone Wmibeat from the git repository, run the following commands:

```
mkdir -p ${GOPATH}/github.com/eskibars
cd ${GOPATH}/github.com/eskibars
git clone https://github.com/eskibars/wmibeat
```


For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).
