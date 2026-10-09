"""Run one validation scenario end to end, from a YAML file in config/scenarios/.

  1. write the simulator config into a fresh run directory (runs/<time>-<name>/)
  2. start the provider simulator and the gateway as separate processes
  3. start the callback receiver and a 1 s metrics sampler
  4. send the load; apply scheduled limit changes in the safe order
  5. snapshot the gateway at the end of the load, then wait for it to drain
  6. fetch batch status and results through the public API
  7. stop everything gracefully (so logs are flushed), then verify and write the report
"""

import asyncio
import contextlib
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import aiohttp
import yaml

from loadgen.generator import LoadGenerator, LoadSpec, MetricsSampler, drain
from loadgen.receiver import CallbackReceiver
from verify.data import load_run
from verify.report import write_report
from verify.run_checks import run_checks

PYTHON_DIR = Path(__file__).resolve().parent.parent
REPO = PYTHON_DIR.parent
HOST = "127.0.0.1"


def free_port() -> int:
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


async def wait_healthy(url: str, proc: subprocess.Popen, timeout_s: float = 30) -> None:
    end = time.monotonic() + timeout_s
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < end:
            if proc.poll() is not None:
                raise RuntimeError(f"{url} exited with code {proc.returncode}; see its log in the run directory")
            with contextlib.suppress(aiohttp.ClientError):
                async with session.get(url + "/healthz") as resp:
                    if resp.status == 200:
                        return
            await asyncio.sleep(0.2)
    raise RuntimeError(f"{url} did not become healthy in {timeout_s}s")


def stop(proc: subprocess.Popen, timeout_s: float = 20) -> None:
    """SIGINT first so the process flushes its logs and closes SQLite cleanly."""
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


class ScenarioRun:
    def __init__(self, scenario: dict, run_root: Path) -> None:
        self.s = scenario
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.dir = run_root / f"{stamp}-{scenario['name']}"
        self.dir.mkdir(parents=True, exist_ok=False)
        self.sim_port, self.gw_port, self.cb_port = free_port(), free_port(), free_port()
        self.sim_url = f"http://{HOST}:{self.sim_port}"
        self.gw_url = f"http://{HOST}:{self.gw_port}"
        self.run_info: dict = {
            "scenario": scenario["name"],
            "description": scenario.get("description", "").strip(),
            "warmup_s": scenario.get("warmup_s", 60),
            "utilization": scenario.get("utilization", 0.97),
            "checks": scenario.get("checks", {}),
            "config": scenario,
            "limit_changes_applied": [],
            "environment": {"cpus": os.cpu_count(), "processor": platform.processor()},
        }

    # --- processes -------------------------------------------------------------

    def _spawn(self, name: str, args: list[str]) -> subprocess.Popen:
        log = open(self.dir / f"{name}.log", "w")
        return subprocess.Popen([sys.executable, "-m", *args], cwd=PYTHON_DIR, stdout=log, stderr=subprocess.STDOUT)

    def start_processes(self) -> None:
        sim_config = self.dir / "models.yaml"
        sim_config.write_text(yaml.safe_dump({
            "seed": self.s.get("seed", 42), "hang_s": self.s.get("hang_s", 30), "models": self.s["models"],
        }))
        self.sim = self._spawn("simulator", [
            "simulator", "--config", str(sim_config), "--host", HOST, "--port", str(self.sim_port),
            "--call-log", str(self.dir / "provider-calls.jsonl"),
        ])
        gateway = self.s.get("gateway", {})
        self.gw = self._spawn("gateway", [
            "gateway", "--config", str(sim_config), "--provider-url", self.sim_url, "--host", HOST,
            "--port", str(self.gw_port), "--db", str(self.dir / "gateway.db"),
            "--utilization", str(self.run_info["utilization"]),
            "--deadline-s", str(gateway.get("deadline_s", 120)),
        ])
        self.gw_pid = self.gw.pid

    # --- limit changes -----------------------------------------------------------

    async def _change_limits(self, session: aiohttp.ClientSession, change: dict, current: dict) -> None:
        model = change["model"]
        new = {"rpm": change.get("rpm", current[model]["rpm"]), "tpm": change.get("tpm", current[model]["tpm"])}
        decrease = new["rpm"] < current[model]["rpm"] or new["tpm"] < current[model]["tpm"]

        async def gateway():
            async with session.put(f"{self.gw_url}/v1/models/{model}/limits", json=new) as resp:
                resp.raise_for_status()

        async def provider():
            async with session.put(f"{self.sim_url}/admin/models/{model}", json=new) as resp:
                resp.raise_for_status()

        # Lowering: tell the gateway first, so it never sends more than the provider's new
        # limit. Raising: tell the provider first, so the gateway never sends into a limit
        # the provider hasn't raised yet.
        t = time.time() - self.t0
        for step in (gateway, provider) if decrease else (provider, gateway):
            await step()
        current[model] = new
        self.run_info["limit_changes_applied"].append({"t": t, "model": model, **new,
                                                       "order": "gateway first" if decrease else "provider first"})
        print(f"  t={t:6.1f}s  {model} limits -> rpm {new['rpm']:,}, tpm {new['tpm']:,}", flush=True)

    async def _apply_changes(self) -> None:
        current = {m: {"rpm": cfg["rpm"], "tpm": cfg["tpm"]} for m, cfg in self.s["models"].items()}
        async with aiohttp.ClientSession() as session:
            for change in sorted(self.s.get("limit_changes", []), key=lambda c: c["at_s"]):
                await asyncio.sleep(max(0.0, self.t0 + change["at_s"] - time.time()))
                await self._change_limits(session, change, current)

    # --- the run ---------------------------------------------------------------------

    async def _snapshot(self) -> dict:
        async with aiohttp.ClientSession() as session, session.get(f"{self.gw_url}/v1/metrics") as resp:
            return await resp.json()

    async def _progress(self, sampler: MetricsSampler) -> None:
        while True:
            await asyncio.sleep(15)
            m = sampler.last
            if m is None:
                continue
            per_model = "  ".join(
                f"{name}: last60 {s['last_60s']['requests']:,}/{s['limits']['rpm']:,} q {s['queue']['waiting']:,}"
                for name, s in m["models"].items()
            )
            print(f"  t={time.time() - self.t0:6.1f}s  {per_model}", flush=True)

    async def _fetch_batches(self, batch_ids: list[str]) -> None:
        statuses, results = {}, {}
        async with aiohttp.ClientSession() as session:
            for bid in batch_ids:
                async with session.get(f"{self.gw_url}/v1/batches/{bid}") as resp:
                    statuses[bid] = await resp.json()
                ids, cursor = [], None
                while True:
                    params = {"limit": 5000} | ({"cursor": cursor} if cursor else {})
                    async with session.get(f"{self.gw_url}/v1/batches/{bid}/results", params=params) as resp:
                        page = await resp.json()
                    ids += [r["request_id"] for r in page["results"]]
                    cursor = page["next_cursor"]
                    if cursor is None:
                        break
                results[bid] = ids
        (self.dir / "batch_status.json").write_text(json.dumps(statuses, indent=2))
        (self.dir / "batch_results.json").write_text(json.dumps(results))

    async def run(self) -> bool:
        print(f"run directory: {self.dir}", flush=True)
        self.start_processes()
        receiver = CallbackReceiver(HOST, self.cb_port, fail_first=self.s.get("callback", {}).get("fail_first", 0),
                                    log_path=self.dir / "callbacks.jsonl")
        try:
            await wait_healthy(self.sim_url, self.sim)
            await wait_healthy(self.gw_url, self.gw)
            await receiver.start()
            load = self.s["load"]
            spec = LoadSpec(
                rate=load["rate"], duration_s=load["duration_s"], models=load["models"],
                batch_size=load.get("batch_size", 1), tokens=load.get("tokens", 1000),
                token_jitter=load.get("token_jitter", 0.0), seed=load.get("seed", 1),
                callback_url=receiver.url if load.get("callback", False) else None,
                deadline_s=load.get("deadline_s"),
            )
            sampler = MetricsSampler(self.gw_url, self.dir / "gateway_timeline.jsonl")
            sampler_task = asyncio.create_task(sampler.run())
            print(f"load: {spec.rate:,} req/s for {spec.duration_s:.0f}s, batch size {spec.batch_size}, "
                  f"models {spec.models}", flush=True)

            self.t0 = time.time()
            side_tasks = [asyncio.create_task(self._apply_changes()), asyncio.create_task(self._progress(sampler))]
            result = await LoadGenerator(self.gw_url, spec).run()
            self.run_info["snapshot_load_end"] = await self._snapshot()
            print(f"load finished: {result.counts()}", flush=True)

            drained = await drain(self.gw_url, self.s.get("drain_timeout_s", 240),
                                  need_callbacks=spec.callback_url is not None)
            # Let the receiver log the final delivery and the sampler take one more sample.
            await asyncio.sleep(1.5)
            self.run_info["snapshot_drained"] = await self._snapshot()
            self.run_info["drained"] = drained
            print("drained" if drained else "WARNING: gateway still busy at drain timeout", flush=True)

            batch_ids = [s.batch_id for s in result.submissions if s.batch_id]
            if 0 < len(batch_ids) <= 20:
                await self._fetch_batches(batch_ids)
                expected = {}
                for sub in result.submissions:
                    if sub.batch_id:
                        expected[sub.batch_id] = [f"{spec.id_prefix}-{sub.index}-{j}" for j in range(sub.n)]
                self.run_info["expected_ids"] = expected

            self.run_info["gateway_restarts"] = 0 if self.gw.poll() is None and self.gw.pid == self.gw_pid else 1
            for task in [*side_tasks, sampler_task]:
                task.cancel()
            loadgen_json = result.to_json()
        finally:
            await receiver.stop()
            stop(self.gw)
            stop(self.sim)

        (self.dir / "loadgen.json").write_text(json.dumps(loadgen_json, indent=2, default=str))
        self.run_info["load_spec"] = asdict(spec)
        (self.dir / "run.json").write_text(json.dumps(self.run_info, indent=2, default=str))

        data = load_run(self.dir)
        checks = run_checks(data)
        report = write_report(data, checks)
        print()
        for check in checks:
            print(f"{'PASS' if check.passed else 'FAIL'}  {check.name}: {check.detail}")
        print(f"\nreport: {report}")
        return all(c.passed for c in checks)


def load_scenario(path: str | Path, overrides: dict) -> dict:
    scenario = yaml.safe_load(Path(path).read_text())
    for key, value in overrides.items():
        if value is not None:
            scenario["load"][key] = value
    return scenario
