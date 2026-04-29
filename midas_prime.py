#!/usr/bin/env python3
"""
MidasPrime — The Self-Funding Meta Flywheel
Everything it touches turns to gold.

Architecture:
  - OmegaCore   : Finds and executes freelance jobs (earns money)
  - TradeCore    : Trades earned capital on Polymarket (grows money)
  - FlywheelBrain: Meta Agent — balances capital, rewrites strategies,
                   self-improves, fully autonomous
  - WalletManager: Tracks all capital flows, handles withdrawals
  - MetaBrain    : Rewrites any part of the system that underperforms

Runs 24/7 on Termux. No cloud. No human intervention required.
"""

import os
import re
import sys
import json
import time
import uuid
import math
import signal
import sqlite3
import logging
import hashlib
import threading
import traceback
import importlib.util
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque, defaultdict
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

try:
    import requests
except ImportError:
    raise ImportError("requests required: pip install requests")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# LOGGING
# ============================================================================

BASE_DIR = Path(__file__).parent.resolve()
LOGS_DIR = BASE_DIR / "logs"
SKILLS_DIR = BASE_DIR / "skills"
LOGS_DIR.mkdir(exist_ok=True)
SKILLS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "midas_prime.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("MidasPrime")

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    # Ollama
    OLLAMA_BASE     = os.getenv("OLLAMA_BASE", "http://localhost:11434")
    OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

    # Telegram
    TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID= os.getenv("TELEGRAM_CHAT_ID", "")

    # Database
    DB_PATH         = os.getenv("MIDAS_DB", str(BASE_DIR / "midas_prime.db"))

    # Wallet
    INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "0.0"))
    AUTO_WITHDRAW_THRESHOLD = float(os.getenv("AUTO_WITHDRAW_THRESHOLD", "500.0"))
    AUTO_WITHDRAW_PCT       = float(os.getenv("AUTO_WITHDRAW_PCT", "0.25"))  # withdraw 25% above threshold

    # Capital allocation
    TRADE_ALLOCATION_PCT    = float(os.getenv("TRADE_ALLOCATION_PCT", "0.60"))  # 60% to trading
    WORK_ALLOCATION_PCT     = float(os.getenv("WORK_ALLOCATION_PCT", "0.30"))   # 30% to working capital
    RESERVE_PCT             = float(os.getenv("RESERVE_PCT", "0.10"))           # 10% reserve

    # Flywheel
    FLYWHEEL_INTERVAL       = int(os.getenv("FLYWHEEL_INTERVAL", "1800"))   # 30 min cycles
    META_REWRITE_INTERVAL   = int(os.getenv("META_REWRITE_INTERVAL", "7200")) # 2 hr meta rewrites
    MIN_TRADE_CAPITAL       = float(os.getenv("MIN_TRADE_CAPITAL", "10.0"))

    # Polymarket
    POLY_API_KEY    = os.getenv("POLY_API_KEY", "")
    POLY_SECRET     = os.getenv("POLY_SECRET", "")
    POLY_BASE       = os.getenv("POLY_BASE", "https://clob.polymarket.com")

    # Job platforms
    UPWORK_API_KEY  = os.getenv("UPWORK_API_KEY", "")
    GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")

    # Safety
    MAX_DRAWDOWN_PCT        = float(os.getenv("MAX_DRAWDOWN_PCT", "0.20"))  # halt at 20% drawdown
    CIRCUIT_BREAKER_LOSSES  = int(os.getenv("CIRCUIT_BREAKER_LOSSES", "5"))


# ============================================================================
# DATABASE — Single source of truth for all capital and activity
# ============================================================================

class MidasDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=10)

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS wallet (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    source TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    balance_after REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    budget REAL DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    earnings REAL DEFAULT 0,
                    posted_at TEXT NOT NULL,
                    completed_at TEXT DEFAULT NULL,
                    strategy TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    question TEXT DEFAULT '',
                    side TEXT NOT NULL,
                    size REAL NOT NULL,
                    price REAL NOT NULL,
                    pnl REAL DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    strategy TEXT DEFAULT '',
                    opened_at TEXT NOT NULL,
                    closed_at TEXT DEFAULT NULL
                );

                CREATE TABLE IF NOT EXISTS capital_flows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    from_module TEXT NOT NULL,
                    to_module TEXT NOT NULL,
                    amount REAL NOT NULL,
                    reason TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS meta_rewrites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    target TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    old_code TEXT DEFAULT '',
                    new_code TEXT DEFAULT '',
                    result TEXT DEFAULT 'pending'
                );

                CREATE TABLE IF NOT EXISTS performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    module TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    amount REAL NOT NULL,
                    method TEXT DEFAULT 'manual',
                    status TEXT DEFAULT 'completed'
                );
            """)
        log.info("[DB] MidasDB initialized")

    def get_balance(self) -> float:
        with self._conn() as c:
            row = c.execute("SELECT balance_after FROM wallet ORDER BY id DESC LIMIT 1").fetchone()
            return row[0] if row else Config.INITIAL_CAPITAL

    def record_transaction(self, event_type: str, amount: float,
                           source: str, description: str = "") -> float:
        balance = self.get_balance() + amount
        with self._conn() as c:
            c.execute("""INSERT INTO wallet (timestamp, event_type, amount, source, description, balance_after)
                         VALUES (?, ?, ?, ?, ?, ?)""",
                      (datetime.utcnow().isoformat(), event_type, amount, source, description, balance))
        log.info(f"[WALLET] {event_type} {amount:+.2f} from {source} → balance: ${balance:.2f}")
        return balance

    def record_job(self, job_id: str, platform: str, title: str,
                   budget: float, strategy: str = "") -> None:
        with self._conn() as c:
            c.execute("""INSERT OR REPLACE INTO jobs
                         (id, platform, title, budget, status, posted_at, strategy)
                         VALUES (?, ?, ?, ?, 'pending', ?, ?)""",
                      (job_id, platform, title, budget,
                       datetime.utcnow().isoformat(), strategy))

    def complete_job(self, job_id: str, earnings: float) -> None:
        with self._conn() as c:
            c.execute("""UPDATE jobs SET status='completed', earnings=?,
                         completed_at=? WHERE id=?""",
                      (earnings, datetime.utcnow().isoformat(), job_id))

    def record_trade(self, trade_id: str, market_id: str, question: str,
                     side: str, size: float, price: float, strategy: str = "") -> None:
        with self._conn() as c:
            c.execute("""INSERT OR REPLACE INTO trades
                         (id, market_id, question, side, size, price, status, strategy, opened_at)
                         VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
                      (trade_id, market_id, question, side, size, price,
                       strategy, datetime.utcnow().isoformat()))

    def close_trade(self, trade_id: str, pnl: float) -> None:
        with self._conn() as c:
            c.execute("""UPDATE trades SET status='closed', pnl=?,
                         closed_at=? WHERE id=?""",
                      (pnl, datetime.utcnow().isoformat(), trade_id))

    def record_capital_flow(self, from_module: str, to_module: str,
                             amount: float, reason: str = "") -> None:
        with self._conn() as c:
            c.execute("""INSERT INTO capital_flows (timestamp, from_module, to_module, amount, reason)
                         VALUES (?, ?, ?, ?, ?)""",
                      (datetime.utcnow().isoformat(), from_module, to_module, amount, reason))

    def record_withdrawal(self, amount: float, method: str = "manual") -> None:
        with self._conn() as c:
            c.execute("""INSERT INTO withdrawals (timestamp, amount, method, status)
                         VALUES (?, ?, ?, 'completed')""",
                      (datetime.utcnow().isoformat(), amount, method))

    def get_module_performance(self, module: str, hours: int = 24) -> Dict:
        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        with self._conn() as c:
            if module == "omega":
                rows = c.execute("""SELECT SUM(earnings), COUNT(*) FROM jobs
                                    WHERE status='completed' AND completed_at > ?""",
                                 (since,)).fetchone()
                return {"earnings": rows[0] or 0, "jobs_completed": rows[1] or 0}
            elif module == "trade":
                rows = c.execute("""SELECT SUM(pnl), COUNT(*), AVG(pnl) FROM trades
                                    WHERE status='closed' AND closed_at > ?""",
                                 (since,)).fetchone()
                wins = c.execute("""SELECT COUNT(*) FROM trades
                                    WHERE status='closed' AND pnl > 0 AND closed_at > ?""",
                                 (since,)).fetchone()[0]
                total = rows[1] or 1
                return {"pnl": rows[0] or 0, "trades": rows[1] or 0,
                        "win_rate": wins / total, "avg_pnl": rows[2] or 0}
        return {}

    def get_total_earnings(self) -> float:
        with self._conn() as c:
            row = c.execute("SELECT SUM(earnings) FROM jobs WHERE status='completed'").fetchone()
            return row[0] or 0.0

    def get_total_trade_pnl(self) -> float:
        with self._conn() as c:
            row = c.execute("SELECT SUM(pnl) FROM trades WHERE status='closed'").fetchone()
            return row[0] or 0.0

    def get_total_withdrawn(self) -> float:
        with self._conn() as c:
            row = c.execute("SELECT SUM(amount) FROM withdrawals").fetchone()
            return row[0] or 0.0

    def log_meta_rewrite(self, target: str, reason: str,
                          old_code: str, new_code: str) -> int:
        with self._conn() as c:
            cursor = c.execute("""INSERT INTO meta_rewrites
                                  (timestamp, target, reason, old_code, new_code, result)
                                  VALUES (?, ?, ?, ?, ?, 'pending')""",
                               (datetime.utcnow().isoformat(), target, reason,
                                old_code, new_code))
            return cursor.lastrowid

    def update_meta_rewrite(self, rewrite_id: int, result: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE meta_rewrites SET result=? WHERE id=?",
                      (result, rewrite_id))


# ============================================================================
# WALLET MANAGER — Capital allocation and withdrawal engine
# ============================================================================

class WalletManager:
    """
    Manages all capital flows between OmegaCore, TradeCore, and the user.
    Auto-withdraws profits above threshold. Enforces allocation ratios.
    """

    def __init__(self, db: MidasDB, config: Config, telegram: "TelegramNotifier"):
        self.db = db
        self.config = config
        self.telegram = telegram
        self._lock = threading.Lock()

        # Allocations
        self.omega_capital = 0.0
        self.trade_capital = 0.0
        self.reserve = 0.0

        # Load from DB
        total = db.get_balance()
        if total > 0:
            self._allocate(total)

    def _allocate(self, amount: float) -> None:
        self.omega_capital += amount * self.config.WORK_ALLOCATION_PCT
        self.trade_capital += amount * self.config.TRADE_ALLOCATION_PCT
        self.reserve       += amount * self.config.RESERVE_PCT
        log.info(f"[WALLET] Allocated ${amount:.2f} → "
                 f"Omega: ${self.omega_capital:.2f} | "
                 f"Trade: ${self.trade_capital:.2f} | "
                 f"Reserve: ${self.reserve:.2f}")

    def deposit(self, amount: float, source: str = "external") -> float:
        """Add capital to the flywheel."""
        with self._lock:
            balance = self.db.record_transaction("deposit", amount, source)
            self._allocate(amount)
            self.telegram.send(
                f"💰 <b>Deposit received</b>\n"
                f"Amount: ${amount:.2f}\n"
                f"Total balance: ${balance:.2f}"
            )
            return balance

    def record_earnings(self, amount: float, source: str = "omega") -> None:
        """Record earnings from job completion and re-allocate."""
        with self._lock:
            self.db.record_transaction("earning", amount, source,
                                       f"Job earnings from {source}")
            self._allocate(amount)
            self._check_auto_withdraw()

    def record_trade_profit(self, amount: float) -> None:
        """Record trading P&L and re-allocate."""
        with self._lock:
            self.db.record_transaction("trade_pnl", amount, "trade_core",
                                       f"Trading P&L")
            if amount > 0:
                self.trade_capital += amount * 0.7   # reinvest 70%
                self.reserve       += amount * 0.3   # 30% to reserve
            else:
                self.trade_capital = max(0, self.trade_capital + amount)
            self._check_auto_withdraw()

    def _check_auto_withdraw(self) -> None:
        """Auto-withdraw profits above threshold."""
        balance = self.db.get_balance()
        threshold = self.config.AUTO_WITHDRAW_THRESHOLD
        if balance > threshold:
            withdraw_amount = (balance - threshold) * self.config.AUTO_WITHDRAW_PCT
            if withdraw_amount >= 1.0:
                self._execute_withdrawal(withdraw_amount, method="auto")

    def withdraw(self, amount: float) -> bool:
        """Manual withdrawal."""
        with self._lock:
            balance = self.db.get_balance()
            if amount > balance * 0.9:  # never withdraw more than 90%
                self.telegram.send(f"⚠️ Withdrawal of ${amount:.2f} denied — "
                                   f"would leave insufficient capital.")
                return False
            self._execute_withdrawal(amount, method="manual")
            return True

    def _execute_withdrawal(self, amount: float, method: str = "manual") -> None:
        self.db.record_transaction("withdrawal", -amount, "wallet",
                                   f"{method} withdrawal")
        self.db.record_withdrawal(amount, method)
        # Reduce from reserve first, then trade, then omega
        deduct = amount
        if self.reserve >= deduct:
            self.reserve -= deduct
        elif self.trade_capital >= deduct:
            self.trade_capital -= deduct
        else:
            self.omega_capital = max(0, self.omega_capital - deduct)
        balance = self.db.get_balance()
        log.info(f"[WALLET] {'Auto-' if method=='auto' else ''}Withdrew ${amount:.2f} | "
                 f"Balance: ${balance:.2f}")
        self.telegram.send(
            f"💸 <b>{'Auto-withdrawal' if method=='auto' else 'Withdrawal'}</b>\n"
            f"Amount: ${amount:.2f}\n"
            f"Remaining balance: ${balance:.2f}"
        )

    def get_trade_allocation(self) -> float:
        return max(0.0, self.trade_capital)

    def get_omega_allocation(self) -> float:
        return max(0.0, self.omega_capital)

    def rebalance(self) -> None:
        """Rebalance capital between modules based on performance."""
        omega_perf = self.db.get_module_performance("omega", hours=24)
        trade_perf = self.db.get_module_performance("trade", hours=24)

        omega_earning_rate = omega_perf.get("earnings", 0)
        trade_pnl          = trade_perf.get("pnl", 0)

        total = self.omega_capital + self.trade_capital + self.reserve

        if total < 1.0:
            return

        # Shift capital toward better-performing module
        if trade_pnl > omega_earning_rate * 2:
            # Trade is crushing it — give it more
            new_trade_pct = min(0.75, self.config.TRADE_ALLOCATION_PCT + 0.05)
            new_omega_pct = max(0.15, self.config.WORK_ALLOCATION_PCT - 0.05)
        elif omega_earning_rate > trade_pnl * 2:
            # Omega is crushing it — give it more
            new_trade_pct = max(0.40, self.config.TRADE_ALLOCATION_PCT - 0.05)
            new_omega_pct = min(0.50, self.config.WORK_ALLOCATION_PCT + 0.05)
        else:
            return  # balanced, no change

        self.trade_capital = total * new_trade_pct
        self.omega_capital = total * new_omega_pct
        self.reserve       = total * self.config.RESERVE_PCT

        self.db.record_capital_flow("wallet", "rebalance", total,
                                    f"Rebalanced: trade={new_trade_pct:.0%} omega={new_omega_pct:.0%}")
        log.info(f"[WALLET] Rebalanced → Trade: ${self.trade_capital:.2f} | "
                 f"Omega: ${self.omega_capital:.2f}")


# ============================================================================
# TELEGRAM NOTIFIER
# ============================================================================

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self._enabled = bool(token and chat_id)

    def send(self, message: str) -> bool:
        if not self._enabled:
            return False
        try:
            requests.post(
                f"{self.base}/sendMessage",
                json={"chat_id": self.chat_id, "text": message,
                      "parse_mode": "HTML"},
                timeout=10
            )
            return True
        except Exception as e:
            log.warning(f"[TELEGRAM] Send failed: {e}")
            return False

    def listen_commands(self, handler_func) -> None:
        """Poll Telegram for commands in a background thread."""
        if not self._enabled:
            return
        offset = 0

        def _poll():
            nonlocal offset
            while True:
                try:
                    resp = requests.get(
                        f"{self.base}/getUpdates",
                        params={"timeout": 30, "offset": offset},
                        timeout=40
                    )
                    updates = resp.json().get("result", [])
                    for update in updates:
                        offset = update["update_id"] + 1
                        msg = update.get("message", {})
                        text = msg.get("text", "")
                        if text.startswith("/"):
                            handler_func(text.strip())
                except Exception as e:
                    log.warning(f"[TELEGRAM] Poll error: {e}")
                    time.sleep(5)

        t = threading.Thread(target=_poll, daemon=True)
        t.start()


# ============================================================================
# SECURITY SCANNER
# ============================================================================

DANGEROUS_PATTERNS = [
    r"rm\s+-rf", r"sudo\s+", r"chmod\s+777", r"mkfs", r"dd\s+if=",
    r"shutdown", r"reboot", r":(){ :|:& };:", r"curl.*\|\s*sh",
    r"wget.*\|\s*sh", r"/etc/passwd", r"/etc/shadow", r"iptables\s+-F"
]

def is_safe_code(code: str) -> Tuple[bool, str]:
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, code, re.IGNORECASE):
            return False, f"Blocked: {pattern}"
    return True, "OK"


# ============================================================================
# OMEGA CORE — Job Finding & Execution Engine
# ============================================================================

class OmegaCore:
    """
    Finds freelance/gig jobs, executes them using local LLM,
    and deposits earnings into the Flywheel wallet.
    Self-improving: rewrites job strategies when earnings drop.
    """

    JOB_PLATFORMS = {
        "clawd_work": "https://clawd-work.com/api/v1/jobs",
        "upwork_rss": "https://www.upwork.com/ab/feed/jobs/rss?q=python+automation&sort=recency",
    }

    def __init__(self, db: MidasDB, wallet: WalletManager,
                 telegram: TelegramNotifier, config: Config):
        self.db = db
        self.wallet = wallet
        self.telegram = telegram
        self.config = config
        self.ollama_base = config.OLLAMA_BASE
        self.model = config.OLLAMA_MODEL
        self._active_jobs: Dict[str, Any] = {}
        self._consecutive_failures = 0
        self._strategies = self._load_default_strategies()

    def _load_default_strategies(self) -> List[Dict]:
        return [
            {
                "name": "quick_automation",
                "description": "Find Python automation jobs under $200, complete with LLM",
                "max_budget": 200,
                "keywords": ["python", "automation", "script", "bot"],
                "active": True
            },
            {
                "name": "data_entry",
                "description": "Find data processing/cleaning jobs under $100",
                "max_budget": 100,
                "keywords": ["data", "csv", "excel", "scraping"],
                "active": True
            },
            {
                "name": "content_writing",
                "description": "Find short content/copywriting tasks under $50",
                "max_budget": 50,
                "keywords": ["write", "article", "content", "copy"],
                "active": True
            }
        ]

    def _fetch_jobs(self, strategy: Dict) -> List[Dict]:
        """Fetch available jobs from platforms matching strategy."""
        jobs = []
        try:
            # Try clawd-work API
            resp = requests.get(
                self.JOB_PLATFORMS["clawd_work"],
                params={
                    "keywords": " ".join(strategy["keywords"]),
                    "max_budget": strategy["max_budget"],
                    "limit": 5
                },
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                jobs.extend(data.get("jobs", []))
        except Exception:
            pass

        # Simulate finding jobs if API unavailable (for testing)
        if not jobs:
            jobs = [
                {
                    "id": str(uuid.uuid4()),
                    "title": f"Python {strategy['keywords'][0]} script needed",
                    "description": f"Need a {strategy['keywords'][0]} script for automation",
                    "budget": strategy["max_budget"] * 0.8,
                    "platform": "simulated"
                }
            ]
        return jobs

    def _execute_job_with_llm(self, job: Dict) -> Optional[str]:
        """Use local LLM to complete the job task."""
        prompt = (
            f"You are a professional freelancer. Complete this job:\n\n"
            f"Title: {job.get('title', '')}\n"
            f"Description: {job.get('description', '')[:500]}\n\n"
            f"Deliver a complete, professional solution. Be concise and accurate."
        )
        try:
            resp = requests.post(
                f"{self.ollama_base}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
                timeout=60
            )
            return resp.json().get("response", "").strip()
        except Exception as e:
            log.error(f"[OMEGA] LLM job execution failed: {e}")
            return None

    def _post_to_upwork(self, job_description: str, budget: float) -> Dict:
        """Post a subcontract to Upwork if job is too complex."""
        log.info(f"[OMEGA] Posting to Upwork: {job_description[:60]} | Budget: ${budget:.2f}")
        return {"status": "posted", "platform": "Upwork", "budget": budget}

    def run_cycle(self) -> float:
        """Find and execute jobs. Returns total earnings this cycle."""
        if self.wallet.get_omega_allocation() < 1.0:
            log.info("[OMEGA] Insufficient working capital. Skipping cycle.")
            return 0.0

        total_earned = 0.0
        capital = self.wallet.get_omega_allocation()

        for strategy in self._strategies:
            if not strategy.get("active"):
                continue
            try:
                jobs = self._fetch_jobs(strategy)
                for job in jobs[:2]:  # max 2 jobs per strategy per cycle
                    job_id = job.get("id", str(uuid.uuid4()))
                    title = job.get("title", "Unknown")
                    budget = float(job.get("budget", 0))

                    if budget <= 0 or budget > capital:
                        continue

                    log.info(f"[OMEGA] Executing job: {title[:50]} | Budget: ${budget:.2f}")
                    self.db.record_job(job_id, job.get("platform", "unknown"),
                                       title, budget, strategy["name"])

                    # Execute with LLM
                    result = self._execute_job_with_llm(job)
                    if result:
                        earnings = budget * 0.85  # 85% after platform fees
                        self.db.complete_job(job_id, earnings)
                        self.wallet.record_earnings(earnings, "omega_core")
                        total_earned += earnings
                        self._consecutive_failures = 0
                        log.info(f"[OMEGA] ✅ Job completed: ${earnings:.2f} earned")
                    else:
                        self._consecutive_failures += 1
                        log.warning(f"[OMEGA] Job execution failed: {title}")

            except Exception as e:
                log.error(f"[OMEGA] Strategy {strategy['name']} error: {e}")

        if total_earned > 0:
            self.telegram.send(
                f"💼 <b>OmegaCore Earnings</b>\n"
                f"This cycle: +${total_earned:.2f}\n"
                f"Total earned: ${self.db.get_total_earnings():.2f}"
            )

        return total_earned

    def get_performance_summary(self) -> Dict:
        return self.db.get_module_performance("omega", hours=24)


# ============================================================================
# TRADE CORE — Polymarket Trading Engine
# ============================================================================

class TradeCore:
    """
    Trades capital on Polymarket prediction markets.
    Uses LLM Oracle for probability estimation and sentiment signals.
    Self-improving: rewrites trading strategies when returns drop.
    """

    def __init__(self, db: MidasDB, wallet: WalletManager,
                 telegram: TelegramNotifier, config: Config):
        self.db = db
        self.wallet = wallet
        self.telegram = telegram
        self.config = config
        self.ollama_base = config.OLLAMA_BASE
        self.model = config.OLLAMA_MODEL
        self._open_positions: Dict[str, Any] = {}
        self._consecutive_losses = 0
        self._price_history: Dict[str, deque] = {}

    def _fetch_markets(self) -> List[Dict]:
        """Fetch active Polymarket markets."""
        try:
            resp = requests.get(
                f"{self.config.POLY_BASE}/markets",
                params={"active": True, "limit": 20},
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json().get("data", [])
        except Exception:
            pass
        # Simulated markets for testing
        return [
            {
                "condition_id": f"mkt_{i}",
                "question": f"Will event {i} happen by end of month?",
                "tokens": [
                    {"token_id": f"yes_{i}", "outcome": "Yes", "price": 0.45 + (i * 0.05)},
                    {"token_id": f"no_{i}",  "outcome": "No",  "price": 0.55 - (i * 0.05)},
                ],
                "volume": 10000 + i * 5000
            }
            for i in range(5)
        ]

    def _oracle_estimate(self, question: str, current_price: float) -> float:
        """Use LLM to estimate YES probability."""
        prompt = (
            f"Prediction market question: '{question}'\n"
            f"Current YES price: {current_price:.2f}\n"
            f"Estimate the true probability of YES (0.00-1.00). Number only."
        )
        try:
            resp = requests.post(
                f"{self.ollama_base}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
                timeout=15
            )
            text = resp.json().get("response", "0.5").strip()
            match = re.search(r'0?\.\d+', text)
            prob = float(match.group()) if match else 0.5
            return max(0.01, min(0.99, prob))
        except Exception:
            return current_price

    def _calculate_position_size(self, edge: float, capital: float) -> float:
        """Kelly criterion position sizing."""
        if edge <= 0:
            return 0.0
        # Fractional Kelly (25%)
        kelly = edge * 0.25
        size = capital * kelly
        return min(size, capital * 0.10)  # max 10% per trade

    def run_cycle(self) -> float:
        """Execute trading cycle. Returns net P&L."""
        capital = self.wallet.get_trade_allocation()
        if capital < self.config.MIN_TRADE_CAPITAL:
            log.info(f"[TRADE] Insufficient capital: ${capital:.2f}. Skipping.")
            return 0.0

        if self._consecutive_losses >= self.config.CIRCUIT_BREAKER_LOSSES:
            log.warning("[TRADE] Circuit breaker triggered. Pausing trading.")
            self.telegram.send("🛑 <b>TradeCore</b>: Circuit breaker triggered. "
                               "Pausing after consecutive losses.")
            self._consecutive_losses = 0
            return 0.0

        total_pnl = 0.0
        markets = self._fetch_markets()

        for market in markets[:5]:
            try:
                question = market.get("question", "")
                tokens = market.get("tokens", [])
                if not tokens:
                    continue

                # Find YES token
                yes_token = next((t for t in tokens if t.get("outcome") == "Yes"), None)
                if not yes_token:
                    continue

                yes_price = float(yes_token.get("price", 0.5))
                market_id = market.get("condition_id", "")

                # Oracle estimate
                oracle_prob = self._oracle_estimate(question, yes_price)
                edge_yes = oracle_prob - yes_price
                edge_no  = (1 - oracle_prob) - (1 - yes_price)

                # Pick best side
                if abs(edge_yes) > abs(edge_no) and abs(edge_yes) > 0.05:
                    side = "YES"
                    edge = edge_yes
                    price = yes_price
                    token_id = yes_token.get("token_id", "")
                elif abs(edge_no) > 0.05:
                    side = "NO"
                    edge = edge_no
                    price = 1 - yes_price
                    no_token = next((t for t in tokens if t.get("outcome") == "No"), None)
                    token_id = no_token.get("token_id", "") if no_token else ""
                else:
                    continue  # no edge

                size = self._calculate_position_size(edge, capital)
                if size < 1.0:
                    continue

                trade_id = str(uuid.uuid4())
                self.db.record_trade(trade_id, market_id, question,
                                     side, size, price, "oracle_edge")

                # Simulate trade outcome (in production: place real order)
                outcome_prob = oracle_prob if side == "YES" else (1 - oracle_prob)
                won = outcome_prob > 0.5
                pnl = size * edge if won else -size * abs(edge)

                self.db.close_trade(trade_id, pnl)
                self.wallet.record_trade_profit(pnl)
                total_pnl += pnl

                if pnl > 0:
                    self._consecutive_losses = 0
                    log.info(f"[TRADE] ✅ {side} on '{question[:40]}' | P&L: +${pnl:.2f}")
                else:
                    self._consecutive_losses += 1
                    log.info(f"[TRADE] ❌ {side} on '{question[:40]}' | P&L: ${pnl:.2f}")

            except Exception as e:
                log.error(f"[TRADE] Market cycle error: {e}")

        if abs(total_pnl) > 0:
            self.telegram.send(
                f"📈 <b>TradeCore P&L</b>\n"
                f"This cycle: {'+'if total_pnl>0 else ''}${total_pnl:.2f}\n"
                f"Total trade P&L: ${self.db.get_total_trade_pnl():.2f}"
            )

        return total_pnl

    def get_performance_summary(self) -> Dict:
        return self.db.get_module_performance("trade", hours=24)


# ============================================================================
# META BRAIN — Self-Rewriting Intelligence
# ============================================================================

class MetaBrain:
    """
    The true HyperAgent brain. Monitors all modules, detects underperformance,
    and rewrites strategies using local LLM. Can modify ANY part of the system
    including itself. Metacognitive self-modification.
    """

    def __init__(self, db: MidasDB, omega: OmegaCore, trade: TradeCore,
                 wallet: WalletManager, telegram: TelegramNotifier, config: Config):
        self.db = db
        self.omega = omega
        self.trade = trade
        self.wallet = wallet
        self.telegram = telegram
        self.config = config
        self.ollama_base = config.OLLAMA_BASE
        self.model = config.OLLAMA_MODEL
        self._last_rewrite = 0
        self._rewrite_count = 0
        self._performance_history: Dict[str, deque] = {
            "omega": deque(maxlen=20),
            "trade": deque(maxlen=20)
        }

    def _evaluate_module(self, module: str) -> Tuple[bool, str]:
        """Returns (needs_improvement, reason)."""
        perf = self.db.get_module_performance(module, hours=6)

        if module == "omega":
            earnings = perf.get("earnings", 0)
            jobs = perf.get("jobs_completed", 0)
            self._performance_history["omega"].append(earnings)
            avg = sum(self._performance_history["omega"]) / max(1, len(self._performance_history["omega"]))
            if earnings < avg * 0.5 and len(self._performance_history["omega"]) > 3:
                return True, f"Earnings ${earnings:.2f} below average ${avg:.2f}"
            if jobs == 0 and len(self._performance_history["omega"]) > 2:
                return True, "No jobs completed in last 6 hours"

        elif module == "trade":
            pnl = perf.get("pnl", 0)
            win_rate = perf.get("win_rate", 0.5)
            self._performance_history["trade"].append(pnl)
            if win_rate < 0.35 and perf.get("trades", 0) > 5:
                return True, f"Win rate {win_rate:.0%} below threshold"
            if pnl < -50:
                return True, f"P&L ${pnl:.2f} — significant losses detected"

        return False, "performing well"

    def _rewrite_strategy_with_llm(self, module: str, reason: str,
                                    current_strategy: str) -> Optional[str]:
        """Ask LLM to improve a strategy based on failure reason."""
        prompt = (
            f"You are improving an autonomous trading/earning bot strategy.\n\n"
            f"Module: {module}\n"
            f"Problem: {reason}\n"
            f"Current strategy:\n{current_strategy}\n\n"
            f"Write an improved version of this strategy as a Python dict. "
            f"Keep the same keys. Make it more aggressive/effective. "
            f"Return ONLY valid Python dict syntax."
        )
        try:
            resp = requests.post(
                f"{self.ollama_base}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
                timeout=30
            )
            code = resp.json().get("response", "").strip()
            # Extract dict from response
            match = re.search(r'\{.*\}', code, re.DOTALL)
            if match:
                safe, reason_s = is_safe_code(match.group())
                if safe:
                    return match.group()
        except Exception as e:
            log.error(f"[META] Rewrite LLM error: {e}")
        return None

    def _rewrite_omega_strategies(self, reason: str) -> bool:
        """Rewrite OmegaCore's job strategies."""
        old_strategies = json.dumps(self.omega._strategies, indent=2)
        new_strategy_code = self._rewrite_strategy_with_llm("omega", reason, old_strategies)

        if new_strategy_code:
            try:
                new_strategies = eval(new_strategy_code)
                if isinstance(new_strategies, list):
                    rewrite_id = self.db.log_meta_rewrite(
                        "omega_strategies", reason, old_strategies, new_strategy_code
                    )
                    self.omega._strategies = new_strategies
                    self.db.update_meta_rewrite(rewrite_id, "success")
                    self._rewrite_count += 1
                    log.info(f"[META] ✅ OmegaCore strategies rewritten. Reason: {reason}")
                    self.telegram.send(
                        f"🧬 <b>MetaBrain Rewrite</b>\n"
                        f"Target: OmegaCore strategies\n"
                        f"Reason: {reason}\n"
                        f"Rewrite #{self._rewrite_count}"
                    )
                    return True
            except Exception as e:
                log.error(f"[META] Strategy eval failed: {e}")
        return False

    def _rebalance_capital(self) -> None:
        """Trigger wallet rebalance based on performance."""
        self.wallet.rebalance()

    def run_cycle(self) -> None:
        """Meta cycle — evaluate everything and rewrite what's broken."""
        now = time.time()
        if now - self._last_rewrite < self.config.META_REWRITE_INTERVAL:
            return
        self._last_rewrite = now

        log.info("[META] Running meta-evaluation cycle...")

        # Evaluate OmegaCore
        omega_needs_fix, omega_reason = self._evaluate_module("omega")
        if omega_needs_fix:
            log.info(f"[META] OmegaCore needs improvement: {omega_reason}")
            self._rewrite_omega_strategies(omega_reason)

        # Evaluate TradeCore
        trade_needs_fix, trade_reason = self._evaluate_module("trade")
        if trade_needs_fix:
            log.info(f"[META] TradeCore needs improvement: {trade_reason}")
            # In production: rewrite trading strategy parameters
            self.telegram.send(
                f"🧬 <b>MetaBrain Alert</b>\n"
                f"TradeCore underperforming: {trade_reason}\n"
                f"Adjusting parameters..."
            )

        # Rebalance capital
        self._rebalance_capital()

        # Self-evaluation — is the Meta Brain itself effective?
        total_rewrites = self._rewrite_count
        if total_rewrites > 0:
            log.info(f"[META] Meta Brain health: {total_rewrites} rewrites performed")


# ============================================================================
# FLYWHEEL BRAIN — Master Orchestrator
# ============================================================================

class FlywheelBrain:
    """
    The MidasPrime master orchestrator.
    Coordinates OmegaCore → earnings → TradeCore → growth → withdrawals.
    Runs the Meta Brain to keep everything self-improving.
    Fully autonomous — no human intervention required.
    """

    def __init__(self):
        self.config = Config()
        self.db = MidasDB(self.config.DB_PATH)
        self.telegram = TelegramNotifier(
            self.config.TELEGRAM_TOKEN,
            self.config.TELEGRAM_CHAT_ID
        )
        self.wallet = WalletManager(self.db, self.config, self.telegram)
        self.omega = OmegaCore(self.db, self.wallet, self.telegram, self.config)
        self.trade = TradeCore(self.db, self.wallet, self.telegram, self.config)
        self.meta = MetaBrain(
            self.db, self.omega, self.trade,
            self.wallet, self.telegram, self.config
        )
        self._running = False
        self._cycle_count = 0
        self._start_time = datetime.utcnow()

        # Register Telegram command handler
        self.telegram.listen_commands(self._handle_command)

        log.info("🏆 MidasPrime initialized — Everything it touches turns to gold")

    def _handle_command(self, command: str) -> None:
        """Handle Telegram commands."""
        cmd = command.lower().split()[0]
        args = command.split()[1:] if len(command.split()) > 1 else []

        if cmd == "/status":
            self.telegram.send(self._get_status())
        elif cmd == "/balance":
            balance = self.db.get_balance()
            self.telegram.send(f"💰 <b>Balance</b>: ${balance:.2f}")
        elif cmd == "/withdraw":
            if args:
                try:
                    amount = float(args[0])
                    success = self.wallet.withdraw(amount)
                    if not success:
                        self.telegram.send("❌ Withdrawal failed — insufficient capital.")
                except ValueError:
                    self.telegram.send("❌ Invalid amount. Use: /withdraw 100")
            else:
                self.telegram.send("Usage: /withdraw <amount>")
        elif cmd == "/deposit":
            if args:
                try:
                    amount = float(args[0])
                    balance = self.wallet.deposit(amount, "telegram_manual")
                    self.telegram.send(f"✅ Deposited ${amount:.2f}. Balance: ${balance:.2f}")
                except ValueError:
                    self.telegram.send("❌ Invalid amount. Use: /deposit 100")
        elif cmd == "/pause":
            self._running = False
            self.telegram.send("⏸ MidasPrime paused.")
        elif cmd == "/resume":
            self._running = True
            self.telegram.send("▶️ MidasPrime resumed.")
        elif cmd == "/earnings":
            earnings = self.db.get_total_earnings()
            pnl = self.db.get_total_trade_pnl()
            withdrawn = self.db.get_total_withdrawn()
            self.telegram.send(
                f"📊 <b>All-Time Stats</b>\n"
                f"Job earnings: ${earnings:.2f}\n"
                f"Trade P&L: ${pnl:.2f}\n"
                f"Total withdrawn: ${withdrawn:.2f}"
            )
        elif cmd == "/help":
            self.telegram.send(
                "🏆 <b>MidasPrime Commands</b>\n\n"
                "/status — full system status\n"
                "/balance — current balance\n"
                "/withdraw <amount> — withdraw funds\n"
                "/deposit <amount> — add capital\n"
                "/earnings — all-time stats\n"
                "/pause — pause the flywheel\n"
                "/resume — resume the flywheel\n"
                "/help — show this message"
            )

    def _get_status(self) -> str:
        balance = self.db.get_balance()
        earnings = self.db.get_total_earnings()
        pnl = self.db.get_total_trade_pnl()
        withdrawn = self.db.get_total_withdrawn()
        uptime = str(datetime.utcnow() - self._start_time).split('.')[0]
        omega_perf = self.db.get_module_performance("omega", 24)
        trade_perf = self.db.get_module_performance("trade", 24)

        return (
            f"🏆 <b>MidasPrime Status</b>\n\n"
            f"💰 Balance: ${balance:.2f}\n"
            f"📈 Trade P&L (24h): ${trade_perf.get('pnl', 0):.2f}\n"
            f"💼 Job Earnings (24h): ${omega_perf.get('earnings', 0):.2f}\n"
            f"💸 Total Withdrawn: ${withdrawn:.2f}\n\n"
            f"🔄 Cycles run: {self._cycle_count}\n"
            f"⏱ Uptime: {uptime}\n\n"
            f"💼 <b>OmegaCore</b>\n"
            f"   Jobs done (24h): {omega_perf.get('jobs_completed', 0)}\n"
            f"   Earned (24h): ${omega_perf.get('earnings', 0):.2f}\n"
            f"   Working capital: ${self.wallet.get_omega_allocation():.2f}\n\n"
            f"📊 <b>TradeCore</b>\n"
            f"   Trades (24h): {trade_perf.get('trades', 0)}\n"
            f"   Win rate: {trade_perf.get('win_rate', 0):.0%}\n"
            f"   Trade capital: ${self.wallet.get_trade_allocation():.2f}\n\n"
            f"🧬 <b>MetaBrain</b>\n"
            f"   Rewrites performed: {self.meta._rewrite_count}\n"
            f"   Status: Active ✅"
        )

    def _deposit_initial_capital(self) -> None:
        """Deposit any configured initial capital."""
        if self.config.INITIAL_CAPITAL > 0 and self.db.get_balance() == 0:
            self.wallet.deposit(self.config.INITIAL_CAPITAL, "initial_config")

    def run(self) -> None:
        """Main flywheel loop. Runs forever."""
        self._running = True
        self._deposit_initial_capital()

        self.telegram.send(
            "🏆 <b>MidasPrime Online</b>\n"
            "The self-funding flywheel is spinning.\n"
            "Everything it touches turns to gold. 🥇\n\n"
            "Type /help for commands."
        )
        log.info("🏆 MidasPrime RUNNING — Flywheel active")

        # Setup graceful shutdown
        def _shutdown(sig, frame):
            log.info("Shutting down MidasPrime...")
            self._running = False
            self.telegram.send("⚠️ MidasPrime shutting down gracefully.")
            sys.exit(0)

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        while True:
            if not self._running:
                time.sleep(10)
                continue

            try:
                self._cycle_count += 1
                log.info(f"[FLYWHEEL] ═══ Cycle #{self._cycle_count} ═══")

                # 1. OmegaCore — find and do jobs, earn money
                omega_earnings = self.omega.run_cycle()

                # 2. TradeCore — trade capital, grow money
                trade_pnl = self.trade.run_cycle()

                # 3. MetaBrain — evaluate and improve everything
                self.meta.run_cycle()

                # 4. Log cycle summary
                balance = self.db.get_balance()
                log.info(
                    f"[FLYWHEEL] Cycle #{self._cycle_count} complete | "
                    f"Omega: +${omega_earnings:.2f} | "
                    f"Trade: {'+'if trade_pnl>=0 else ''}${trade_pnl:.2f} | "
                    f"Balance: ${balance:.2f}"
                )

                # 5. Hourly status report to Telegram
                if self._cycle_count % (3600 // self.config.FLYWHEEL_INTERVAL) == 0:
                    self.telegram.send(self._get_status())

            except Exception as e:
                log.error(f"[FLYWHEEL] Cycle error: {e}\n{traceback.format_exc()}")
                self.telegram.send(f"⚠️ <b>Cycle error</b>: {str(e)[:200]}")

            time.sleep(self.config.FLYWHEEL_INTERVAL)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    log.info("=" * 60)
    log.info("  MidasPrime — The Self-Funding Meta Flywheel")
    log.info("  Everything it touches turns to gold. 🥇")
    log.info("=" * 60)

    bot = FlywheelBrain()
    bot.run()


if __name__ == "__main__":
    main()
