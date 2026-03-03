from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import EngineAdapter
from ..process import run_command, run_shell_command, CommandResult
from ..utils import truncate_error


class MonetDBAdapter(EngineAdapter):
    name = "monetdb"

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        super().__init__(config, global_cfg, resource, logger)
        self.native_cfg = config.get("native") or {}
        self.host = self.native_cfg.get("host", "localhost")
        self.port = int(self.native_cfg.get("port", 50000))
        self.user = self.native_cfg.get("user", "monetdb")
        self.password = self.native_cfg.get("password", "monetdb")
        self.dbname = self.native_cfg.get("dbname", "tpcds_sf10")
        self.mclient_path = self.native_cfg.get("mclient_path", "mclient")
        self.monetdb_path = self.native_cfg.get("monetdb_path", "monetdb")
        self.monetdbd_path = self.native_cfg.get("monetdbd_path", "monetdbd")
        self.farm_path = self.native_cfg.get("farm_path", "/var/lib/monetdb")
        self.start_command = self.native_cfg.get("start_command")
        self.stop_command = self.native_cfg.get("stop_command")

    def start(self) -> None:
        if self.start_command:
            result = run_shell_command(
                self.start_command,
                env=self.env,
                resource=self.resource,
                use_memory_limit=False,
            )
            if result.returncode != 0:
                msg = truncate_error(result.stderr or result.stdout)
                raise RuntimeError(f"MonetDB start failed: {msg}")
        self._wait_for_ready()

    def stop(self) -> None:
        if self.stop_command:
            run_shell_command(
                self.stop_command,
                env=self.env,
                resource=self.resource,
                use_memory_limit=False,
            )
        super().stop()

    def _wait_for_ready(self) -> None:
        retries = int(self.native_cfg.get("startup_retries", 15))
        delay = float(self.native_cfg.get("startup_delay_seconds", 2.0))
        for _ in range(retries):
            result = run_command(
                self._mclient_command("SELECT 1;"),
                env=self.env,
            )
            if result.returncode == 0:
                return
            import time
            time.sleep(delay)
        raise RuntimeError("MonetDB did not become ready in time.")

    def supports_planning(self) -> bool:
        return True

    def build_sql(self, sql: str, stage: str) -> str:
        if stage == "planning":
            return f"EXPLAIN {sql}"
        return sql

    def build_command(self, sql: str, stage: str) -> List[str]:
        return self._mclient_command(sql)

    def _mclient_command(self, sql: str) -> List[str]:
        # Use the pymonetdb wrapper which handles auth consistently
        # script path: experiments/scripts/run_pymonetdb.py
        # venv python: .venv/bin/python3
        
        # Resolve paths relative to project root (cwd)
        wrapper_path = "experiments/scripts/run_pymonetdb.py"
        python_path = ".venv/bin/python3"
        
        # Usage: run_pymonetdb.py <dbname> <sql> <autocommit=0|1>
        # We use autocommit=1 for queries (default) unless transaction needed?
        # Adapter usually just runs one-off queries.
        # But for correctness, maybe autocommit=1 is fine.
        
        return [
            python_path,
            wrapper_path,
            self.dbname,
            sql,
            "0" # is_file=0 for raw SQL strings
        ]

    def check_alive(self) -> bool:
        # Pymonetdb wrapper takes approx 1-2s to start, so simple select 1 is fine
        result = run_command(
            self._mclient_command("SELECT 1;"),
            env=self.env,
            timeout_s=10,
        )
        return result.returncode == 0
    
    def version(self) -> str:
        # mserver5 might still work? Or use pymonetdb to query version?
        # SELECT value FROM sys.env() WHERE name = 'monet_version';
        # Let's try to query it.
        # If fallback to mserver5, that's fine.
        return super().version() # Uses mclient/mserver5 logic from base if any? No, it was custom.
        # Original version() used mserver5 --version. That should be fine if mserver5 is in PATH.
        # Revert to original implementation for version() as it doesn't need auth.
        result = run_command(["mserver5", "--version"], env=self.env)
        out = (result.stdout or result.stderr or "").strip()
        for line in out.splitlines():
            if "MonetDB" in line:
                return line.strip()
        return out.splitlines()[0] if out else "unknown"
