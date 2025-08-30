import time, pytest, sd_index_manager as sim
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

@pytest.mark.integration
@pytest.mark.launch
def test_webui_launch_invokes_uvicorn(monkeypatch):
    monkeypatch.setattr('builtins.input', lambda prompt='': str(BASE_DIR))
    calls = {}
    def fake_run(cmd, check=True):
        calls['cmd'] = cmd; time.sleep(0.01)
        class R: returncode = 0
        return R()
    import subprocess
    monkeypatch.setattr(subprocess, 'run', fake_run)
    sim.run_webui()
    assert 'cmd' in calls and any('uvicorn' in c for c in calls['cmd'])
