# 📋 GCS Analytics Core - Review Guidelines

## 🎯 Purpose & Scope
This guide ensures code quality, performance, and maintainability for the `gcs-analytics-core` library. Reviewers (including AI assistants) must validate all changes against these principles, prioritizing **stability, performance, and ecosystem compatibility**.

---

## 🌍 1. Open Source & Ecosystem Compatibility
*The library operates as a critical I/O layer in environments like Apache Spark, Iceberg, Hudi, and Hadoop. Breaking changes or inefficiency can cause system-wide failures.*

*   **License Compliance:** Every new file **MUST** start with the Apache License 2.0 header. No exceptions.
*   **Dependency Hygiene:** Do not introduce new dependencies without license validation (Apache 2.0 preferred). Avoid dependency bloat to minimize classpath conflicts in Spark/Hadoop environments (shaded jars are preferred if internal shading is used).
*   **Binary & Semantic Compatibility:** Public APIs in `client` and `core` must maintain backward compatibility. Do not change method signatures; prefer `default` interface methods or deprecation cycles.
*   **No Internal State Leaks:** Ensure internal classes remain package-private to prevent users from binding to unstable internals.
*   **Security & Data Governance:** **NEVER** log, expose, or commit credentials, access tokens, or private keys. Be extremely careful about logging complete GCS URIs or object names at `INFO` level or higher if they might contain sensitive user data (PII). Sanitize exception messages to ensure they don't leak internal tokens.

---

## ⚡ 2. Big Data Performance & Resource Management
*Code runs in high-concurrency, high-throughput environments. GC pressure and I/O bottlenecks are critical.*

*   **Zero-Copy & Buffer Efficiency:** Prefer Vectored I/O and direct buffers where applicable. Minimize buffer copies.
*   **I/O Overhead:** Ensure optimizations like prefetching or Parquet footer optimizations are guarded and fail gracefully if the file format is unexpected.
*   **Strict Resource Cleanup:** All `AutoCloseable` resources (streams, channels, clients) **MUST** use `try-with-resources`. Never rely on finalizers.
*   **GC Pressure:** Avoid allocating large objects or buffers inside tight loops (e.g., in `read()` loops). Reuse buffers where thread safety allows.
*   **Streams API in Hot Paths:** Do **NOT** use Java Streams, lambdas, or method references in performance-critical inner loops (e.g., the data plane). They introduce object allocation overhead and GC pressure. Use traditional primitive loops.
*   **Efficient Logging:** Avoid string concatenation in log statements (e.g., `log.debug("Read " + size)`). Use parameterized logging (`log.debug("Read {}", size)`). Avoid logging in the data plane unless it is a critical error.
*   **Telemetry & Metrics Overhead:** Metric collection **MUST** have near-zero overhead. Avoid lock contention when updating counters (use `LongAdder` or `Atomic` variables instead of `synchronized` blocks). Consider sampling for high-frequency events rather than capturing 100% of data points if performance drops.

---

## 🧵 3. Concurrency & Thread Safety
*Filesystem operations are invoked concurrently by multiple Spark tasks/threads.*

*   **Immutable State:** Prefer immutable value types (`@AutoValue`). Minimize shared mutable state.
*   **Thread Safety:** Ensure `GcsFileSystem`, `GcsClient`, and custom I/O streams are thread-safe if shared across tasks. Use appropriate synchronization primitives or Guava concurrency utilities.
*   **Locking:** Keep critical sections small. Prefer lock-free structures or fine-grained locking over `synchronized` blocks.

---

## 🛠️ 4. Google Best Practices & Code Style
*The project adheres strictly to Google Java Style and standards.*

*   **Guava Utilities:** Use `Preconditions` for arguments and `Verify` for internal state. Do not invent custom validation logic.
*   **Null Safety:** Strictly annotate methods and parameters with `@Nullable` or `@NonNull`. Prefer `Optional` for return types over `null` where appropriate (avoid `Optional` in performance-critical inner loops).
*   **Fluent APIs:** Use Builder patterns for complex object creation to improve readability.
*   **Fail-Fast:** Validate inputs at the boundary (public methods) immediately.
*   **Java Imports:** Never use wildcard imports (`import java.util.*`). Limit `static` imports to well-known, self-describing methods/constants from standard libraries (e.g., `Preconditions.checkNotNull`, `Truth.assertThat`). Do **NOT** statically import constants, variables, or deep-import nested enums/classes from internal or non-standard libraries where the enclosing class provides essential context (e.g., prefer `GcsAnalyticsCoreTelemetryConstants.Metric` over importing `Metric` directly). Keep imports sorted per Google Style.
*   **Streams API for Readability:** Outside of hot paths (e.g., in control plane or configuration logic), use Streams API for clear, declarative collection processing. Prefer explicit loops if the pipeline becomes overly complex (more than 3-4 transformations).

---

## 🧪 5. Testing Rigor
*Testing must reflect real-world usage without being brittle.*

*   **Fakes over Mocks:** Use the provided fakes in `test-lib` instead of mocking GCS interactions where possible.
*   **Assertion Style:** Use Google **Truth** (`assertThat`) for all assertions. Do not use JUnit `assertEquals`.
*   **AAA Pattern:** Enforce strict Arrange-Act-Assert structure in tests.
*   **Coverage:** Ensure high-sensitivity I/O paths have near 100% coverage, including error paths (e.g., simulated GCS timeouts or disconnects).

---

## 📝 6. PR & Commit Hygiene
*Commit history is read by the public community and automation.*

*   **Conventional Commits:** Ensure PR titles follow Conventional Commits (e.g., `feat(core): ...`, `fix(client): ...`) to drive `release-please`.
*   **Atomic Changes:** Keep PRs focused. Do not mix refactoring, formatting, and feature implementation in one PR.
*   **Commit "Why":** Commit messages should explain the *motivation* (why), not just the *implementation* (what).

---

## 🤖 Instructions for Gemini Code Assist
When reviewing or generating code for this project, you **MUST**:
1.  Verify the Apache 2.0 license header is present.
2.  Check for potential memory leaks or unclosed resources.
3.  Ensure no proprietary libraries or unapproved dependencies are used.
4.  Scrutinize I/O paths for performance bottlenecks (e.g., excessive seeking or blocking).
5.  Enforce Google Java Style and Guava usage.
