"""
blackroad-business-intelligence: Business metrics dashboard with KPI tracking.
SQLite persistence at ~/.blackroad/business-intelligence.db
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional

# ── ANSI colours ─────────────────────────────────────────────────────────────
GREEN   = "\033[92m"
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
RESET   = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "business-intelligence.db"

KPI_CATEGORIES = ["revenue", "operations", "marketing", "product", "finance", "hr", "custom"]
AGGREGATION_TYPES = ["sum", "avg", "max", "min", "last"]


# ── Models ────────────────────────────────────────────────────────────────────
@dataclass
class KPI:
    name: str
    category: str
    unit: str
    target: float
    description: str = ""
    aggregation: str = "last"
    tags: List[str] = field(default_factory=list)
    id: Optional[int] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class MetricEntry:
    kpi_id: int
    value: float
    period: str = ""
    note: str = ""
    id: Optional[int] = None
    recorded_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def __post_init__(self):
        if not self.period:
            self.period = date.today().strftime("%Y-%m")


@dataclass
class KPIWithMetrics:
    kpi: KPI
    latest_value: Optional[float]
    previous_value: Optional[float]
    all_time_high: Optional[float]
    entry_count: int

    def achievement_pct(self) -> Optional[float]:
        if self.latest_value is None or self.kpi.target == 0:
            return None
        return round(self.latest_value / self.kpi.target * 100, 1)

    def trend(self) -> str:
        if self.latest_value is None or self.previous_value is None:
            return "—"
        delta = self.latest_value - self.previous_value
        if abs(delta) < 0.001:
            return "→"
        return "↑" if delta > 0 else "↓"

    def trend_color(self) -> str:
        t = self.trend()
        return GREEN if t == "↑" else RED if t == "↓" else YELLOW

    def achievement_color(self) -> str:
        pct = self.achievement_pct()
        if pct is None:
            return DIM
        if pct >= 100:
            return GREEN
        if pct >= 75:
            return YELLOW
        return RED


# ── Core logic ────────────────────────────────────────────────────────────────
class BIEngine:
    """Business Intelligence engine for KPI definition and metric tracking."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kpis (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    name        TEXT NOT NULL,
                    category    TEXT NOT NULL,
                    unit        TEXT NOT NULL,
                    target      REAL DEFAULT 0,
                    description TEXT DEFAULT '',
                    aggregation TEXT DEFAULT 'last',
                    tags        TEXT DEFAULT '[]',
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_entries (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    kpi_id      INTEGER NOT NULL,
                    value       REAL NOT NULL,
                    period      TEXT NOT NULL,
                    note        TEXT DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    FOREIGN KEY (kpi_id) REFERENCES kpis(id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kpis_category ON kpis(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_kpi ON metric_entries(kpi_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_period ON metric_entries(period)")
            conn.commit()

    def add_kpi(self, name: str, category: str, unit: str, target: float,
                description: str = "", aggregation: str = "last",
                tags: Optional[List[str]] = None) -> KPI:
        kpi = KPI(name=name, category=category, unit=unit, target=target,
                  description=description, aggregation=aggregation,
                  tags=tags or [])
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO kpis
                   (name, category, unit, target, description, aggregation, tags, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (kpi.name, kpi.category, kpi.unit, kpi.target, kpi.description,
                 kpi.aggregation, json.dumps(kpi.tags), kpi.created_at, kpi.updated_at),
            )
            kpi.id = cur.lastrowid
            conn.commit()
        return kpi

    def record_metric(self, kpi_id: int, value: float,
                      period: str = "", note: str = "") -> MetricEntry:
        entry = MetricEntry(kpi_id=kpi_id, value=value, period=period, note=note)
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO metric_entries (kpi_id, value, period, note, recorded_at)
                   VALUES (?,?,?,?,?)""",
                (entry.kpi_id, entry.value, entry.period, entry.note, entry.recorded_at),
            )
            entry.id = cur.lastrowid
            conn.execute("UPDATE kpis SET updated_at=? WHERE id=?",
                         (entry.recorded_at, kpi_id))
            conn.commit()
        return entry

    def list_kpis(self, category: Optional[str] = None) -> List[KPIWithMetrics]:
        query = "SELECT * FROM kpis"
        params: list = []
        if category:
            query += " WHERE category = ?"
            params.append(category)
        query += " ORDER BY category, name"
        with self._connect() as conn:
            kpi_rows = conn.execute(query, params).fetchall()
            result = []
            for row in kpi_rows:
                kpi = self._row_to_kpi(row)
                metrics = conn.execute(
                    "SELECT value FROM metric_entries WHERE kpi_id=? ORDER BY recorded_at DESC LIMIT 2",
                    (kpi.id,),
                ).fetchall()
                ath = conn.execute(
                    "SELECT MAX(value) FROM metric_entries WHERE kpi_id=?", (kpi.id,)
                ).fetchone()[0]
                cnt = conn.execute(
                    "SELECT COUNT(*) FROM metric_entries WHERE kpi_id=?", (kpi.id,)
                ).fetchone()[0]
                latest = metrics[0]["value"] if metrics else None
                previous = metrics[1]["value"] if len(metrics) > 1 else None
                result.append(KPIWithMetrics(
                    kpi=kpi, latest_value=latest, previous_value=previous,
                    all_time_high=ath, entry_count=cnt,
                ))
        return result

    def get_kpi_history(self, kpi_id: int, limit: int = 12) -> List[MetricEntry]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM metric_entries WHERE kpi_id=?
                   ORDER BY recorded_at DESC LIMIT ?""",
                (kpi_id, limit),
            ).fetchall()
        return [MetricEntry(id=r["id"], kpi_id=r["kpi_id"], value=r["value"],
                            period=r["period"], note=r["note"],
                            recorded_at=r["recorded_at"]) for r in rows]

    def export_csv(self, output_path: Optional[Path] = None) -> str:
        kpis_with_metrics = self.list_kpis()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["KPI", "Category", "Unit", "Target", "Latest",
                         "Achievement%", "Trend", "ATH", "Entries"])
        for km in kpis_with_metrics:
            pct = km.achievement_pct()
            writer.writerow([
                km.kpi.name, km.kpi.category, km.kpi.unit,
                km.kpi.target,
                km.latest_value if km.latest_value is not None else "",
                f"{pct:.1f}" if pct is not None else "",
                km.trend(), km.all_time_high or "", km.entry_count,
            ])
        content = buf.getvalue()
        if output_path:
            Path(output_path).write_text(content)
        return content

    def get_dashboard_summary(self) -> dict:
        kpis_with_metrics = self.list_kpis()
        on_target = sum(1 for km in kpis_with_metrics
                        if (km.achievement_pct() or 0) >= 100)
        at_risk = sum(1 for km in kpis_with_metrics
                      if 75 <= (km.achievement_pct() or 0) < 100)
        off_track = sum(1 for km in kpis_with_metrics
                        if (km.achievement_pct() or 0) < 75 and km.latest_value is not None)
        cats: dict[str, int] = {}
        for km in kpis_with_metrics:
            cats[km.kpi.category] = cats.get(km.kpi.category, 0) + 1
        return {
            "total_kpis": len(kpis_with_metrics),
            "on_target": on_target,
            "at_risk": at_risk,
            "off_track": off_track,
            "no_data": len(kpis_with_metrics) - on_target - at_risk - off_track,
            "by_category": cats,
        }

    @staticmethod
    def _row_to_kpi(r: sqlite3.Row) -> KPI:
        return KPI(id=r["id"], name=r["name"], category=r["category"],
                   unit=r["unit"], target=r["target"], description=r["description"],
                   aggregation=r["aggregation"], tags=json.loads(r["tags"]),
                   created_at=r["created_at"], updated_at=r["updated_at"])


# ── Terminal rendering ────────────────────────────────────────────────────────
def _print_header(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 70}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 70}{RESET}\n")


def _progress_bar(pct: Optional[float], width: int = 20) -> str:
    if pct is None:
        return DIM + "░" * width + RESET
    filled = min(int(pct / 100 * width), width)
    color = GREEN if pct >= 100 else YELLOW if pct >= 75 else RED
    return color + "█" * filled + DIM + "░" * (width - filled) + RESET


def _print_kpi(km: KPIWithMetrics) -> None:
    pct = km.achievement_pct()
    pct_str = f"{pct:.1f}%" if pct is not None else "no data"
    val_str = f"{km.latest_value:,.2f}" if km.latest_value is not None else "—"
    trend_c = km.trend_color()
    ach_c   = km.achievement_color()

    print(f"  {BOLD}{GREEN}{km.kpi.name}{RESET}  {DIM}[{km.kpi.category}]{RESET}")
    print(f"  {_progress_bar(pct)}  "
          f"{ach_c}{pct_str}{RESET}  "
          f"val: {BOLD}{val_str} {km.kpi.unit}{RESET}  "
          f"target: {km.kpi.target:,} {km.kpi.unit}  "
          f"trend: {trend_c}{km.trend()}{RESET}")


def _print_dashboard(engine: BIEngine) -> None:
    summary = engine.get_dashboard_summary()
    _print_header("📊  Business Intelligence — Dashboard")
    print(f"  {YELLOW}Total KPIs  :{RESET}  {summary['total_kpis']}")
    print(f"  {GREEN}On target   :{RESET}  {summary['on_target']}")
    print(f"  {YELLOW}At risk     :{RESET}  {summary['at_risk']}")
    print(f"  {RED}Off track   :{RESET}  {summary['off_track']}")
    print(f"  {DIM}No data     :{RESET}  {summary['no_data']}")
    if summary["by_category"]:
        print(f"\n  {BOLD}KPIs by category:{RESET}")
        for cat, cnt in sorted(summary["by_category"].items()):
            bar = "█" * min(cnt, 20)
            print(f"    {cat:<16} {CYAN}{bar}{RESET} {cnt}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="business-intelligence",
        description="BlackRoad Business Intelligence — KPI tracker",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List KPIs with latest values")
    p_list.add_argument("-c", "--category", choices=KPI_CATEGORIES)

    p_add = sub.add_parser("add", help="Define a new KPI")
    p_add.add_argument("name")
    p_add.add_argument("category", choices=KPI_CATEGORIES)
    p_add.add_argument("unit")
    p_add.add_argument("target", type=float)
    p_add.add_argument("--description", default="")
    p_add.add_argument("--aggregation", default="last", choices=AGGREGATION_TYPES)

    p_rec = sub.add_parser("record", help="Record a metric value")
    p_rec.add_argument("kpi_id", type=int)
    p_rec.add_argument("value", type=float)
    p_rec.add_argument("--period", default="")
    p_rec.add_argument("--note", default="")

    sub.add_parser("status", help="Dashboard summary")

    p_exp = sub.add_parser("export", help="Export KPI data to CSV")
    p_exp.add_argument("-o", "--output", default="kpi_export.csv")

    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    engine = BIEngine()

    if args.command == "list":
        kpis = engine.list_kpis(category=args.category)
        _print_header(f"📊  KPIs  ({len(kpis)} tracked)")
        if not kpis:
            print(f"  {DIM}No KPIs defined yet. Use 'add' to create one.{RESET}\n")
        for km in kpis:
            _print_kpi(km)
            print()

    elif args.command == "add":
        kpi = engine.add_kpi(args.name, args.category, args.unit, args.target,
                             args.description, args.aggregation)
        print(f"\n{GREEN}✓ KPI added:{RESET} [{kpi.id}] {kpi.name}  "
              f"target: {kpi.target} {kpi.unit}  [{kpi.category}]\n")

    elif args.command == "record":
        entry = engine.record_metric(args.kpi_id, args.value, args.period, args.note)
        print(f"\n{GREEN}✓ Metric recorded:{RESET} KPI #{args.kpi_id}  "
              f"value: {args.value}  period: {entry.period}\n")

    elif args.command == "status":
        _print_dashboard(engine)

    elif args.command == "export":
        engine.export_csv(Path(args.output))
        print(f"\n{GREEN}✓ Exported to:{RESET} {args.output}\n")


if __name__ == "__main__":
    main()
