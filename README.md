# Heron Java DSL starter project

A simple example project for the Heron Java DSL, including the necessary Maven configuration.

## Requirements

You need to have [Maven](https://maven.apache.org) installed.

## Building a topology JAR

To build a JAR (with dependencies):

```bash
$ mvn assembly:assembly
```

That will create a "fat" topology JAR in `target/heron-java-dsl-example-0.1.0-jar-with-dependencies.jar`.

## Submitting the topology to Heron

To submit the example topology to a Heron cluster:

```bash
$ heron submit local target/heron-java-dsl-example-0.1.0-jar-with-dependencies.jar \
  io.streaml.heron.functionalapi.WordCountFunctionalTopology \
  WordCountFunctionalTopology
```
