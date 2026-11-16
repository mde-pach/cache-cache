import sys

import pytest

from cache_cache.check import main, run_check


@pytest.fixture(autouse=True)
def _fresh_import():
    """Each test imports the sample app again, so its functions register again."""
    for name in [m for m in sys.modules if m.startswith("tests.sample_app")]:
        del sys.modules[name]


def test_check_reports_typos_and_unused_tags() -> None:
    report = run_check("tests.sample_app")
    assert report.errors == [
        'tests.sample_app.repo.ProductRepo.typo: invalidates("prodcut:{pid}") matches no '
        'declared tag, did you mean "product:{pid}"?'
    ]
    assert report.warnings == ['tag "order:{oid}" is never invalidated']
    assert not report.ok


def test_cli_exit_code(capsys) -> None:
    assert main(["check", "tests.sample_app"]) == 1
    out = capsys.readouterr().out
    assert "did you mean" in out
    assert 'warning: tag "order:{oid}"' in out


def test_cli_import_error() -> None:
    report = run_check("tests.does_not_exist")
    assert report.errors and "can't import" in report.errors[0]
