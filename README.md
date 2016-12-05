# pentaho-hadoop-shims #
Hadoop Configurations, also known and shims and the Pentaho Big Data Adaptive layer, are collections of Hadoop libraries required to communicate with a specific version of Hadoop (and related tools: Hive, HBase, Sqoop, Pig, etc.). They are designed to be easily configured.

#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 1.8


#### Building it

__Build for nightly/release__

```
$ mvn clean install -Drelease
```

This will build, unit test, and package the whole project (all of the sub-modules). Every submodule in pentaho-hadoop-shims is independent set of libraries and resources for one hadoop vendor.

#### Running the tests

__Unit tests__

This will run all tests in the project (and sub-modules).
```
$ mvn test
```

If you want to remote debug a single java unit test (default port is 5005):
```
$ cd server/core
$ mvn test -Dtest=PlaceTest -Dmaven.surefire.debug
```
