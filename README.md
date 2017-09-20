# pentaho-hadoop-shims #
Hadoop Configurations, also known and shims and the Pentaho Big Data Adaptive layer, are collections of Hadoop libraries required to communicate with a specific version of Hadoop (and related tools: Hive, HBase, Sqoop, Pig, etc.). They are designed to be easily configured.

How to build
--------------

pentaho-hadoop-shims uses the maven framework. 


#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 1.8
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

#### Building it

This is a maven project, and to build it use the following command

```
$ mvn clean install
```
Optionally you can specify -Drelease to trigger obfuscation and/or uglification (as needed)

Optionally you can specify -Dmaven.test.skip=true to skip the tests (even though
you shouldn't as you know)

The build result will be a Pentaho package located in ```target```.

#### Running the tests

__Unit tests__

This will run all unit tests in the project (and sub-modules). To run integration tests as well, see Integration Tests below.

```
$ mvn test
```

If you want to remote debug a single java unit test (default port is 5005):

```
$ cd core
$ mvn test -Dtest=<<YourTest>> -Dmaven.surefire.debug
```

__Running tests on Windows__

Running tests on Window requires additional environment set up because of existing problems running Hadoop on Windows (please see https://wiki.apache.org/hadoop/WindowsProblems).

Exactly it needs to have **hadoop.home.dir** variable pointed to dir with ` \bin\winutils.exe`.

__Steps to set up environment:__
 
 - Download *winutils.exe*  binary. E.g. from: https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-winutils
 - Create any dir (e.g. d:\TEMP_DIR) and **bin** sub dir in it. Then put **winutils.exe** in **bin**:
 `d:\TEMP_DIR\bin\winutils.exe`
 
 - Set system property `hadoop.home.dir="d:\TEMP_DIR"` or just run the Java process with this property:
```
$ mvn test -Dhadoop.home.dir="d:\TEMP_DIR"
```
or
```
$ mvn clean install -Dhadoop.home.dir="d:\TEMP_DIR"
```

__Integration tests__

In addition to the unit tests, there are integration tests that test cross-module operation. This will run the integration tests.

```
$ mvn verify -DrunITs
```

To run a single integration test:

```
$ mvn verify -DrunITs -Dit.test=<<YourIT>>
```

To run a single integration test in debug mode (for remote debugging in an IDE) on the default port of 5005:

```
$ mvn verify -DrunITs -Dit.test=<<YourIT>> -Dmaven.failsafe.debug
```
PentahoMapReduceIT.java

To skip test

```
$ mvn clean install -DskipTests
```

To get log as text file

```
$ mvn clean install test >log.txt
```


__IntelliJ__

* Don't use IntelliJ's built-in maven. Make it use the same one you use from the commandline.
  * Project Preferences -> Build, Execution, Deployment -> Build Tools -> Maven ==> Maven home directory

````
