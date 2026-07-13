import nox

# Force nox to use uv as the default virtualenv backend for blazing-fast speed
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "format", "typecheck", "tests"]

@nox.session(python="3.11")
def lint(session: nox.Session) -> None:
    """Run ruff linter to verify code quality."""
    session.install("ruff")
    session.run("ruff", "check", "danio", "tests")

@nox.session(python="3.11")
def format(session: nox.Session) -> None:
    """Verify code formatting using ruff format."""
    session.install("ruff")
    session.run("ruff", "format", "--check", "danio", "tests")

@nox.session(python="3.11")
def typecheck(session: nox.Session) -> None:
    """Run mypy to verify type correctness across the codebase."""
    # mypy needs dependencies to resolve types correctly
    session.install("-e", ".[test]")
    session.install("mypy")
    session.run("mypy", "--ignore-missing-imports", "danio")

@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def tests(session: nox.Session) -> None:
    """Run tests and code coverage across the full Python version matrix using uv."""
    # Install the package in editable mode with test dependencies
    session.install("-e", ".[test]")
    
    # Run pytest with coverage reporting
    session.run("pytest", "--cov=danio", "tests/", *session.posargs)
