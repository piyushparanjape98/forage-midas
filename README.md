# Midas
Project repo for the JPMC Advanced Software Engineering Forage program

## CI / Integration Tests

- Integration tests that require a running Kafka broker are executed only under the Maven profile `kafka-tests`.
- These tests are configured to use Testcontainers (Docker) for an ephemeral Kafka instance so they run reliably on CI (Linux) and on developer machines with Docker Desktop.

To run integration tests locally with Docker installed:

```powershell
# Runs only when Docker is available; uses the 'kafka-tests' profile which enables Kafka integration tests
.\mvnw.cmd -Pkafka-tests test
```

On CI (GitHub Actions) we run the `kafka-tests` profile on an Ubuntu runner that has Docker available; the workflow file is `.github/workflows/ci-kafka.yml`.

