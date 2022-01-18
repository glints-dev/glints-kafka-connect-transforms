# Kafka Connect Transforms

This is a collection of Kafka Connect transforms used internally within Glints.

## Usage

Drop the JAR into the path given by `plugin.path` in the Kafka Connect
configuration, then use the `transforms.*` options.

## Current Transforms

- `CamelCaseToSnakeCase`: Converts property names from camelCase to snake_case.

## Developing

This project has a `devcontainer.json` file that works with Visual Studio
Code's Remote-Containers extension and GitHub Codespaces.

Simply open the directory in Visual Studio Code and re-open in a container
when prompted - all development dependencies should be available in the
container environment.

To build, use `sbt compile` (Scala Build Tool).

For quick development, you can start an interactive shell using `sbt`, then
run `~test`. This watches files for changes and automatically re-builds and
re-runs tests when changes are detected.

To package as a JAR, run `sbt package`.
