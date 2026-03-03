from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import EngineAdapter
from ..docker_utils import container_exists, container_running, restart_container, run_container, start_container, stop_container
from ..process import run_command, run_shell_command
from ..utils import truncate_error


class ClickHouseAdapter(EngineAdapter):
    name = "clickhouse"

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        super().__init__(config, global_cfg, resource, logger)
        self.native_cfg = config.get("native") or {}
        self.docker_cfg = config.get("docker") or {}
        self.host = self._cfg("host", "localhost")
        self.port = int(self._cfg("port", 9000))
        self.user = self._cfg("user", "default")
        self.password = self._cfg("password", None)
        self.database = self._cfg("database", None)
        self.client_path = self._cfg("client_path", "clickhouse-client")
        self.pid_file = self._cfg("pid_file", None)

    def _cfg(self, key: str, default: Any) -> Any:
        if self.execution_mode == "native":
            return self.native_cfg.get(key, default)
        return self.docker_cfg.get(key, default)

    def start(self) -> None:
        if self.execution_mode == "native":
            self._start_native()
        else:
            self._start_docker()
        self._wait_for_ready()

    def stop(self) -> None:
        if self.execution_mode == "native":
            self._stop_native()
        else:
            self._stop_docker()
        super().stop()

    def _start_native(self) -> None:
        start_command = self.native_cfg.get("start_command")
        start_mode = self.native_cfg.get("start_mode", "service")
        if not start_command:
            self.logger.info("No clickhouse start_command provided; assuming server already running.")
            return

        if start_mode == "process":
            cmd = shlex.split(start_command)
            cgroup = self.resource.create_cgroup("experiments_clickhouse") if self.resource else None
            if cgroup:
                self.server_cgroup = cgroup
            cmd, preexec = self.resource.wrap_command(cmd, use_memory_limit=True, cgroup_handle=cgroup)
            self.server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                preexec_fn=preexec,
                env=self.env,
            )
        else:
            result = run_shell_command(start_command, env=self.env, resource=self.resource, use_memory_limit=False)
            if result.returncode != 0:
                raise RuntimeError(f"ClickHouse start_command failed: {truncate_error(result.stderr)}")

    def _stop_native(self) -> None:
        stop_command = self.native_cfg.get("stop_command")
        if stop_command:
            run_shell_command(stop_command, env=self.env, resource=self.resource, use_memory_limit=False)
        if self.server_process:
            self.server_process.terminate()
            self.server_process.wait(timeout=10)
            self.server_process = None
        if self.server_cgroup:
            self.server_cgroup.cleanup()
            self.server_cgroup = None

    def _start_docker(self) -> None:
        name = self.docker_cfg.get("container_name", "experiments-clickhouse")
        image = self.docker_cfg.get("image")
        if not image:
            raise RuntimeError("ClickHouse docker image not configured.")

        if container_exists(name):
            if not container_running(name):
                start_container(name)
            return

        ports = self.docker_cfg.get("ports") or ["9000:9000", "8123:8123"]
        resource_args = self._docker_resource_args()
        run_container(
            name=name,
            image=image,
            ports=ports,
            env=self.docker_cfg.get("env") or {},
            volumes=self.docker_cfg.get("volumes") or [],
            extra_args=self.docker_cfg.get("extra_args") or [],
            command=self.docker_cfg.get("command"),
            resource_args=resource_args,
        )

    def _stop_docker(self) -> None:
        name = self.docker_cfg.get("container_name", "experiments-clickhouse")
        stop_container(name)

    def restart(self) -> None:
        if self.execution_mode == "docker":
            name = self.docker_cfg.get("container_name", "experiments-clickhouse")
            restart_container(name)
            self._wait_for_ready()
            return
        super().restart()
        self._wait_for_ready()

    def check_alive(self) -> bool:
        if self.execution_mode == "docker":
            name = self.docker_cfg.get("container_name", "experiments-clickhouse")
            return container_running(name)
        if self.server_process:
            return self.server_process.poll() is None
        if self.pid_file:
            pid = self._read_pid_file()
            if pid:
                return Path(f"/proc/{pid}").exists()
        return True

    def monitor_pids(self) -> List[int]:
        pids: List[int] = []
        if self.server_process and self.server_process.pid:
            pids.append(self.server_process.pid)
        if self.pid_file:
            pid = self._read_pid_file()
            if pid:
                pids.append(pid)
        return pids

    def _read_pid_file(self) -> Optional[int]:
        try:
            content = Path(self.pid_file).read_text(encoding="utf-8").strip()
            if content:
                return int(content.split()[0])
        except Exception:
            return None
        return None

    def _wait_for_ready(self) -> None:
        retries = int(self._cfg("startup_retries", 10))
        delay = float(self._cfg("startup_delay_seconds", 1.0))
        for _ in range(retries):
            result = run_command(self._client_command("SELECT 1"), env=self.env)
            if result.returncode == 0:
                pid = self._read_pid_file() if self.pid_file else None
                if pid:
                    self._ensure_cgroup_for_pid(pid)
                return
            msg = truncate_error(result.stderr or result.stdout)
            self.logger.debug("ClickHouse not ready yet: %s", msg)
            import time
            time.sleep(delay)
        raise RuntimeError("ClickHouse did not become ready in time.")

    def _ensure_cgroup_for_pid(self, pid: int) -> None:
        if self.server_cgroup or not self.resource:
            return
        cgroup = self.resource.create_cgroup("experiments_clickhouse_service")
        if not cgroup:
            return
        try:
            self.resource.attach_pid(cgroup, pid)
            self.server_cgroup = cgroup
        except Exception:
            cgroup.cleanup()

    def build_sql(self, sql: str, stage: str) -> str:
        statements = [f"SET max_threads = {self.global_cfg.threads}"]
        if stage == "planning":
            statements.append(f"EXPLAIN {sql}")
        else:
            statements.append(sql)
        return ";\n".join(statements) + ";"

    def build_command(self, sql: str, stage: str) -> List[str]:
        return self._client_command(sql)

    def _client_command(self, sql: str) -> List[str]:
        cmd = [self.client_path]
        cmd += ["--host", str(self.host), "--port", str(self.port), "--multiquery", "--query", sql]
        if self.user:
            cmd += ["--user", str(self.user)]
        if self.password:
            cmd += ["--password", str(self.password)]
        if self.database:
            cmd += ["--database", str(self.database)]
        return cmd

    def version(self) -> str:
        result = run_command(self._client_command("SELECT version()"), env=self.env)
        if result.returncode != 0:
            return "unknown"
        return result.stdout.strip().splitlines()[-1]

    def is_connection_error(self, stderr: str, stdout: str) -> bool:
        msg = f"{stderr}\n{stdout}".lower()
        needles = [
            "connection refused",
            "connection reset",
            "connection failure",
            "connection to server was lost",
            "client has disconnected",
        ]
        return any(n in msg for n in needles)

    def _docker_resource_args(self) -> List[str]:
        args: List[str] = ["--memory", str(self.global_cfg.memory_limit_bytes)]
        if self.global_cfg.cpu_affinity:
            cpus = ",".join(str(c) for c in self.global_cfg.cpu_affinity)
            args += ["--cpuset-cpus", cpus]
        return args
