# Pentaho KTR integration tests

The integration tests execute a real KTR with the Pentaho client Docker image and assert the file output produced inside the container. The test module is gated behind `runIntegrationTests`, so normal builds do not start Docker.

Run the Pentaho 11 ecosystem:

```text
mvn -DrunIntegrationTests -Ppentaho-11 -pl integration-test -am verify
```

Run the Pentaho 10.2 ecosystem separately:

```text
mvn -DrunIntegrationTests -Ppentaho-10.2 -pl integration-test -am verify
```

The profiles intentionally select different Kettle and `automation-utils` artifacts, Docker image tags, and integration-test classes. Do not activate both profiles in one Maven invocation.

The default Pentaho 11 image is `11.1`, while the default 10.2 image is `10.2.0.9`. The Docker registry prefix defaults to `one.hitachivantara.com/pnt-docker/`. To test another 10.2.0.X image, override the image property and provide the matching 10.2 platform artifact when required:

```text
mvn -DrunIntegrationTests -Ppentaho-10.2 \
  -Dpdi-it.pdi-102-image-version=10.2.0.X \
  -Dpdi-it.pdi-102-platform-version=10.2.0.0-SNAPSHOT \
  -pl integration-test -am verify
```

Docker must be running and authenticated to the configured registry, and the configured Pentaho Maven repository must be available. Override `pdi-it.docker-pull-host` when CI uses another registry mirror.