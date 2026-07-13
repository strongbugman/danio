import nox

# Force nox to use uv as the default virtualenv backend for blazing-fast speed
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["tests"]

@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def tests(session: nox.Session) -> None:
    """Run linting, formatting, typechecking, and unit tests for each Python version."""
    # 1. Install editable package with test dependencies plus static analysis tools
    session.install("-e", ".[test]", "ruff", "mypy")
    
    # 2. Run Ruff Linter
    session.run("ruff", "check", "danio", "tests")
    
    # 3. Run Ruff Formatter
    session.run("ruff", "format", "--check", "danio", "tests")
    
    # 4. Run Mypy Typechecker
    session.run("mypy", "--ignore-missing-imports", "danio")
    
    # 5. Run Pytest with coverage reporting
    session.run("pytest", "--cov=danio", "tests/", *session.posargs)
