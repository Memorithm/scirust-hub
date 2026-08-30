from pathlib import Path
import runpy

Path("crates/hub-executor/tests").mkdir(parents=True, exist_ok=True)
runpy.run_path(".github/agent_pr4.py", run_name="__main__")
