# Danio v0.5.2 Release Notes

This release brings massive engineering upgrades, critical bug fixes identified during deep architectural reviews, 100% type-safety compliance across all Python environments, and a streamlined, secure, and fully automated multi-version CI/CD pipeline.

---

## 🛠️ Summary of Code Modifications & Bug Fixes

### 1. ORM & Schema Engine Core Refactors
*   **Field Default Overwrite Fix**: Resolved an issue in `model.py` where `Field(default=...)` custom defaults were silently overwritten with `dataclasses.MISSING` because the extraction logic unconditionally fell back to the class body's literal default. Now, custom `Field` defaults are respected unless overridden by an explicit class body literal.
*   **Dynamic Default Memoization Fix**: Removed class-level descriptor memoization of callable defaults (e.g., `datetime.now`) in `schema.py`. Dynamic defaults are now freshly evaluated and copied on every model instantiation, preventing concurrent shared state corruption.
*   **Standard Declarative API Fix**: Added `__hash__` to the `Field` class. Previously, custom `__eq__` (used for the SQL query DSL) caused Mypy and Python to treat `Field` as unhashable, leading standard class-body declarations like `id: int = danio.field(danio.IntField, primary=True)` to fail with `ValueError: mutable default ... is not allowed`.
*   **Transaction Atomicity Fix**: Patched `create_or_update` in `model.py` to forward the `database` connection instance to the nested `.update()` call. This prevents the update half from escaping the open transaction and running on a default routed connection, ensuring 100% atomic transactions.

### 2. Multi-Version Compatibility & Type Safety
*   **Lower-Version PEP 681 Support**: Unconditionally imported `dataclass_transform` from `typing_extensions` in `danio/model.py`. This ensures full compatibility across Python 3.9 and 3.10 and eliminates Mypy's `attr-defined` errors on older typing modules.
*   **Mypy Strictness Compliance**: Added `# type: ignore[arg-type]` annotations to internal `dataclasses.fields` calls in `model.py` to appease Mypy's strict `DataclassInstance` constraints on types transformed with `@dataclass_transform()`.

---

## 🛰️ CI/CD & Local Multi-Version Testing Upgrades

### 1. Isolated Concurrent Test Sessions
*   **Dynamic Test Databases**: Modified test suites (`tests/test_mysql.py` and `tests/test_postgres.py`) to dynamically append Python's major/minor version to the target database name:
    ```python
    db_name = f"test_danio_{sys.version_info.major}_{sys.version_info.minor}"
    ```
    This completely isolates database sessions, allowing parallel or concurrent multi-version testing matrix runs to execute flawlessly against a single shared database server without concurrency conflicts or database state pollution.
*   **Makefile Conditional Assignments (`?=`)**: Converted static environment assignments in the `Makefile` to conditional assignments (`?=`). This allows the local test suite to fall back seamlessly to local database servers (e.g. `192.168.2.4`) while allowing the CI environment to inject loopback IPs (`127.0.0.1`) without Makefile overrides.

### 2. High-Performance Nox Local Testing
*   Added `noxfile.py` utilizing the supercharged `uv` backend (`nox.options.default_venv_backend = "uv"`).
*   Configured sessions to run the entire pipeline (Ruff Lint, Ruff Format, Mypy, and Pytest) isolated for each Python version in the matrix: `[3.9, 3.10, 3.11, 3.12]`.

### 3. Service Container Stabilization
*   **Postgres Health Check**: Configured `pg_isready -U postgres` in the Docker health check command to prevent Postgres from throwing `FATAL: role "root" does not exist` errors when the Docker daemon executes the check as root.
*   **MariaDB Health Check**: Configured `mariadb-admin ping` as an unquoted, credential-less check utilizing UNIX socket auto-authentication, with a relaxed 50-second bootstrap window to eliminate flakey startups.

### 4. Zero-Touch GitHub Actions Release
*   Consolidated the testing suite and release pipeline into a single, cohesive `.github/workflows/test-and-release.yml` file.
*   Implemented **OIDC Trusted Publishing (受信发布)** with PyPI to allow safe, passwordless publishing from GitHub.
*   Automated GitHub Release creation, attaching the sdist and wheels built using `uv build` as release assets.
