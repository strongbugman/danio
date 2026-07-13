import nox

# Force nox to use uv as the default virtualenv backend for blazing-fast speed
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["tests"]

@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def tests(session: nox.Session) -> None:
    """Run tests and code coverage across the full Python version matrix using uv."""
    # 1. Install the package in editable mode with test dependencies
    session.install("-e", ".[test]")
    
    # 2. Run pytest with coverage reporting
    session.run("pytest", "--cov=danio", "tests/", *session.posargs)
