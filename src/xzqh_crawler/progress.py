"""Terminal progress panel for township crawling.

Design goals
- Live panel in CLI (Rich).
- Aggregate progress by level1 province (code[:2]).
- Track dynamic queue: done/queued, ok/err, current parent_code.

This module is intentionally decoupled from the crawler logic: worker threads only
call small thread-safe methods to report events.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


@dataclass
class ProvinceProgress:
    queued: int = 0
    done: int = 0
    ok: int = 0
    err: int = 0
    l4_nodes: int = 0  # level4 nodes yielded (crawler-side, before DB upsert)
    current: str = ""
    last_update_ts: float = field(default_factory=time.time)


class ProgressReporter:
    def __init__(
        self,
        refresh_per_second: float = 4.0,
        *,
        max_workers: Optional[int] = None,
        province_name_resolver: Optional[Callable[[str], str]] = None,
    ):
        self._lock = threading.Lock()
        self._by_province: Dict[str, ProvinceProgress] = {}
        self._start_ts: float = time.time()
        self._refresh_per_second = refresh_per_second
        self._max_workers = max_workers
        self._province_name_resolver = province_name_resolver

        # Attached Rich Live instance (optional). When set, we can push updates
        # proactively (avoids relying solely on Live's background refresh).
        self._live: Optional[Live] = None

        self._total_queued = 0
        self._total_done = 0
        self._total_ok = 0
        self._total_err = 0
        self._total_l4_nodes = 0
        self._footer: str = ""

    @staticmethod
    def _province_key(parent_code: str) -> str:
        return (parent_code or "")[:2] or "??"

    def add_queued(self, parent_code: str) -> None:
        """Increment queued counter for the corresponding province."""
        prov = self._province_key(parent_code)
        with self._lock:
            p = self._by_province.setdefault(prov, ProvinceProgress())
            p.queued += 1
            p.current = parent_code
            p.last_update_ts = time.time()
            self._total_queued += 1

        # Push a refresh to the Live instance if one is attached.
        self.refresh()

    def mark_done(self, parent_code: str, *, ok: bool) -> None:
        prov = self._province_key(parent_code)
        with self._lock:
            p = self._by_province.setdefault(prov, ProvinceProgress())
            p.done += 1
            if ok:
                p.ok += 1
                self._total_ok += 1
            else:
                p.err += 1
                self._total_err += 1
            p.current = parent_code
            p.last_update_ts = time.time()
            self._total_done += 1

        self.refresh()

    def add_level4_nodes(self, parent_code: str, count: int) -> None:
        """Track level4 node growth (crawler-side, method A).

        Call this when a township list is fetched/parsed, with the number of
        nodes yielded (not the number inserted into DB).
        """
        if count <= 0:
            return
        prov = self._province_key(parent_code)
        with self._lock:
            p = self._by_province.setdefault(prov, ProvinceProgress())
            p.l4_nodes += count
            self._total_l4_nodes += count
            p.current = parent_code
            p.last_update_ts = time.time()

        self.refresh()

    def set_footer(self, text: str) -> None:
        with self._lock:
            self._footer = text
        self.refresh()

    def render(self) -> Panel:
        with self._lock:
            by_prov = dict(self._by_province)
            total_queued = self._total_queued
            total_done = self._total_done
            total_ok = self._total_ok
            total_err = self._total_err
            total_l4_nodes = self._total_l4_nodes
            start_ts = self._start_ts
            footer = self._footer

        elapsed = max(0.001, time.time() - start_ts)
        rps = total_done / elapsed
        in_flight = max(0, total_queued - total_done)

        table = Table(show_header=True, header_style="bold")
        table.add_column("省", style="cyan", width=6)
        table.add_column("地名", style="magenta", width=12)
        table.add_column("完成/队列", justify="right")
        table.add_column("OK", justify="right", style="green")
        table.add_column("ERR", justify="right", style="red")
        table.add_column("L4+", justify="right", style="yellow")
        table.add_column("当前parent_code", overflow="fold")

        for prov in sorted(by_prov.keys()):
            p = by_prov[prov]
            prov_name = ""
            if self._province_name_resolver:
                try:
                    prov_name = self._province_name_resolver(prov)
                except Exception:
                    prov_name = ""
            current_disp = p.current
            parent_name = ""
            if p.current and self._province_name_resolver:
                try:
                    # Reuse resolver: accept full code and return name
                    parent_name = self._province_name_resolver(p.current)
                except Exception:
                    parent_name = ""
            if parent_name:
                current_disp = f"{p.current}({parent_name})"

            table.add_row(
                prov,
                prov_name,
                f"{p.done}/{p.queued}",
                str(p.ok),
                str(p.err),
                str(p.l4_nodes),
                current_disp,
            )

        workers = f" | workers {self._max_workers}" if self._max_workers else ""
        title = Text(
            f"乡级抓取进度 | total {total_done}/{total_queued} | inflight {in_flight}{workers} | ok {total_ok} err {total_err} | L4 +{total_l4_nodes} | {rps:.2f} req/s | {elapsed:.1f}s"
        )

        subtitle = Text(footer) if footer else None
        return Panel(
            table,
            title=title,
            subtitle=subtitle,
            width=None,
            height=None,
            expand=True,
        )

    def refresh(self) -> None:
        """If Live is attached, push an immediate refresh."""
        live = self._live
        if live is None:
            return
        try:
            live.update(self.render(), refresh=True)
        except Exception:
            # Never let progress reporting break crawling
            pass

    def live(self) -> Live:
        # force_terminal ensures Rich uses full-screen rendering even when
        # output is redirected; terminal width/height will be taken from env.
        console = Console(force_terminal=True)
        live = Live(
            self.render(),
            console=console,
            refresh_per_second=self._refresh_per_second,
            transient=False,
            screen=True,
        )
        self._live = live
        return live
