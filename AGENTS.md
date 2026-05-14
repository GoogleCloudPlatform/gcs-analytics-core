# GCS Analytics Core — Agent Instructions

Project conventions, architecture, and coding patterns for the `gcs-analytics-core` library.

## Architecture

### Module Boundaries

- **Client** (`client/`): Optimized GCS client layer. Provides the `GcsFileSystem` and `GcsClient` abstractions, along with Vectored I/O implementations. It is the foundational layer for interacting with GCS.
- **Core** (`core/`): Accelerated I/O stream layer. Implements the `GoogleCloudStorageInputStream`, which provides seekable, optimized access to GCS with features like prefetching and Parquet footer optimization. It builds on top of the `client` module.
- **Common** (`common/`): Shared utilities, constants, and base types used across all modules. Avoid adding complex logic here.
- **Test Lib** (`test-lib/`): Shared testing infrastructure, fakes, and test utilities.
- **Coverage** (`coverage/`): Aggregates code coverage reports across the multi-module project.

### High-Sensitivity Areas

- **I/O Paths**: Logic involving direct GCS interactions or stream processing. Performance and resource management (closing streams) are critical.
- **Concurrency**: Any code using parallel streams, thread pools, or shared mutable state.
- **Public APIs**: Classes in the `client` module or public interfaces in `core`. Stability is key.

## Design Patterns

- **Immutability**: Prefer immutable value types. Use `com.google.auto.value.AutoValue` for data-carrying classes.
- **Guava Utilities**: Use Guava's `Preconditions` for argument validation and `Verify` for internal state checks.
- **Fluent APIs**: Where appropriate, use the builder pattern or fluent interfaces to improve readability.
- **Resource Management**: Always use `try-with-resources` for `AutoCloseable` types (streams, clients, etc.).
- **Error Handling**: Use standard Java exceptions or custom exceptions defined in the project. Avoid swallowing exceptions.

## Coding Conventions

### API Design

- **Visibility**: Keep classes and methods package-private unless they must be public.
- **Javadoc**: Required for all public classes and methods. Follow Google Javadoc style.
- **Nullability**: Use `@Nullable` and `@NonNull` (or equivalent) to document nullability expectations.
- **Interface Stability**: Avoid breaking public interfaces. Add default methods if extending existing interfaces.

### Naming

- Follow standard Java naming conventions.
- Be descriptive: `isOptimizable()` is better than `check()`.
- Avoid abbreviations unless they are industry standard (e.g., `GCS`, `URI`).

### Code Style

- **Google Java Style**: The project strictly follows the Google Java Style Guide.
- **Indentation**: 2 spaces.
- **Line Length**: 100 characters.
- **Spotless**: All code must be formatted via Spotless.
- **Single Responsibility Principle (SRP)**: Each class and method should have one focused responsibility.
- **Focused Methods**: Keep methods short and focused. If a method exceeds 30-40 lines, consider refactoring.
- **Fail-Fast**: Validate arguments and state early using Guava's `Preconditions`.
- **Side-Effect Free**: Prefer pure functions and immutable state where possible to make code more predictable and testable.

### Testing

- **JUnit 5**: Use JUnit 5 for all new tests.
- **Truth**: Use Google Truth for assertions (e.g., `assertThat(actual).isEqualTo(expected)`).
- **Arrange-Act-Assert (AAA)**: Organize test methods into three distinct blocks:
  - **Arrange**: Set up the objects, mocks/fakes, and state.
  - **Act**: Invoke the method or action being tested.
  - **Assert**: Verify that the action had the expected effect.
- **Descriptive Test Names**: Test names should clearly state the condition being tested and the expected outcome (e.g., `read_atEndOfFile_returnsMinusOne`).
- **One Assertion per Test**: Aim for a single logical assertion per test case to keep tests focused and failures easy to diagnose.
- **Fakes vs Mocks**: Prefer using fakes (available in `test-lib`) over mocks for unit tests. Fakes provide more realistic behavior and are less prone to over-specification.
- **Mocking**: Use Mockito only when a suitable fake is not available or for verifying interactions that are difficult to check with fakes.
- **Integration Tests**: Place integration tests in `src/integrationTest/java` and run them with the `integration-test` profile.
- **Benchmarks**: Use JMH for performance-critical code, located in `src/jmh/java`.

## Commands

- **Build without tests**: `./mvnw clean install -DskipTests`
- **Run unit tests**: `./mvnw test`
- **Run integration tests**: `./mvnw verify -Pintegration-test`
- **Run benchmarks**: `./mvnw integration-test -Pjmh`
- **Apply formatting (Spotless)**: `./mvnw spotless:apply`
- **Check formatting (Spotless)**: `./mvnw spotless:check`
- **Check for updates**: `./mvnw versions:display-dependency-updates`

## PR & Commit Conventions

- **License Header**: Every file must start with the Apache License 2.0 header.
- **Commit Messages**: Clear, concise, and explain the "why" behind the change.
- **Atomic PRs**: One feature or bug fix per PR. Avoid mixing refactoring with new features.
- **PR Titles**: Follow [Conventional Commits](https://www.conventionalcommits.org/) format (e.g., `feat: ...`, `fix: ...`, `docs: ...`) to support `release-please`. Include the module scope if applicable, e.g., `feat(core): optimize stream reads`.

## Boundaries

- **Never** disable Error Prone or Spotless checks unless absolutely necessary and documented.
- **Never** add new dependencies without checking for license compatibility (Apache 2.0 preferred).
- **Never** commit secrets or credentials.
- **Ask first** before introducing a new major framework or library.
