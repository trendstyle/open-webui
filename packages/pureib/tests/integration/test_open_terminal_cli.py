"""The --ib option on `open-terminal run` hands the app and address to pureib's server."""

import os
import tempfile

import pytest

os.environ.setdefault('OPEN_TERMINAL_API_KEY', 'pureib-integration-key')
os.environ.setdefault('OPEN_TERMINAL_FILE_BROWSER_ROOT', tempfile.mkdtemp(prefix='pureib-cli-'))
click_testing = pytest.importorskip('click.testing')
ot_cli = pytest.importorskip('open_terminal.cli', reason='open-terminal is not installed')


@pytest.fixture
def served(monkeypatch):
    run_cmd = ot_cli.main.commands.get('run')
    if run_cmd is None or not any(param.name == 'ib' for param in run_cmd.params):
        pytest.skip('this open-terminal build has no --ib option; the patch is not merged')
    calls = []

    def fake_serve(app, address, **options):
        calls.append((app, address))

    monkeypatch.setattr('pureib.http_server.serve_ib', fake_serve)
    return calls


def test_ib_option_serves_the_app_on_the_address(served):
    result = click_testing.CliRunner().invoke(ot_cli.main, ['run', '--ib', 'ib://auto:8000'])
    assert result.exit_code == 0, result.output
    assert served == [('open_terminal.main:app', 'ib://auto:8000')]


def test_ib_option_rejects_combination_with_ipc(served):
    result = click_testing.CliRunner().invoke(
        ot_cli.main, ['run', '--ib', 'ib://auto:8000', '--ipc', '/tmp/pureib-test.sock']
    )
    assert result.exit_code != 0
    assert 'cannot be combined' in result.output
    assert served == []
