from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import psutil

from .resources import ResourceController


@dataclass
class CommandResult:
    stdout: str
    stderr: str
    returncode: Optional[int]
    wall_time_ms: int
    timed_out: bool
    peak_rss_bytes: Optional[int]


def _collect_rss(pids: Iterable[int], stop_event: threading.Event, result_holder: Dict[str, int]) -> None:
    peak = 0
    while not stop_event.is_set():
        total = 0
        for pid in list(pids):
            try:
                proc = psutil.Process(pid)
                procs = [proc] + proc.children(recursive=True)
                for child in procs:
                    try:
                        total += child.memory_info().rss
                    except psutil.Error:
                        continue
            except psutil.Error:
                continue
        if total > peak:
            peak = total
        stop_event.wait(0.05)
    result_holder["peak"] = peak


def run_command(
    cmd: List[str],
    *,
    timeout_s: Optional[int] = None,
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    resource: Optional[ResourceController] = None,
    use_memory_limit: bool = True,
    monitor_pids: Optional[List[int]] = None,
    cgroup_name: Optional[str] = None,
    keep_cgroup: bool = False,
) -> CommandResult:
    cgroup_handle = None
    preexec_fn = None

    if resource and use_memory_limit:
        cgroup_handle = resource.create_cgroup(cgroup_name)
        cmd, preexec_fn = resource.wrap_command(cmd, use_memory_limit=use_memory_limit, cgroup_handle=cgroup_handle)

    start = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        cwd=cwd,
        preexec_fn=preexec_fn,
        start_new_session=True,  # own process group → timeout can reap the whole tree (bash+mclient)
    )

    stop_event = threading.Event()
    peak_holder: Dict[str, int] = {}
    monitor_thread = None
    if monitor_pids:
        monitor_thread = threading.Thread(
            target=_collect_rss,
            args=(monitor_pids, stop_event, peak_holder),
            daemon=True,
        )
        monitor_thread.start()

    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        # Kill the WHOLE process group, not just the immediate child. Queries run via
        # `bash -lc "mclient ..."`, so proc is the bash wrapper; a plain proc.kill() leaves the
        # mclient grandchild — and its server-side query — alive. At SF100 that left MonetDB
        # grinding a single join for hours past the 1800 s timeout and choked every later query.
        # start_new_session=True above put the child in its own group so killpg reaps the tree;
        # closing mclient's connection lets the engine abort the query.
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        proc.kill()
        stdout, stderr = proc.communicate()

    end = time.monotonic()
    stop_event.set()
    if monitor_thread:
        monitor_thread.join(timeout=1.0)

    if cgroup_handle and not keep_cgroup:
        cgroup_handle.cleanup()

    return CommandResult(
        stdout=stdout or "",
        stderr=stderr or "",
        returncode=proc.returncode,
        wall_time_ms=int((end - start) * 1000),
        timed_out=timed_out,
        peak_rss_bytes=peak_holder.get("peak"),
    )


def run_shell_command(
    command: str,
    *,
    timeout_s: Optional[int] = None,
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    resource: Optional[ResourceController] = None,
    use_memory_limit: bool = True,
    monitor_pids: Optional[List[int]] = None,
    cgroup_name: Optional[str] = None,
    keep_cgroup: bool = False,
) -> CommandResult:
    cmd = ["bash", "-lc", command]
    return run_command(
        cmd,
        timeout_s=timeout_s,
        env=env,
        cwd=cwd,
        resource=resource,
        use_memory_limit=use_memory_limit,
        monitor_pids=monitor_pids,
        cgroup_name=cgroup_name,
        keep_cgroup=keep_cgroup,
    )
