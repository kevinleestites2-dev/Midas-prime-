# 🥇 MidasPrime — The Self-Funding Meta Flywheel

> Everything it touches turns to gold.

MidasPrime is a fully autonomous, self-improving earning machine.  
It works to fund itself, trades to grow itself, and pays you whenever you want.  
No cloud. No human intervention. Runs 24/7 on Termux.

---

## How It Works

```
💼 OmegaCore          📈 TradeCore
(finds jobs)    →→→   (trades capital)
(earns money)   ←←←   (grows money)
      ↓                     ↓
   💸 YOUR WALLET (withdraw anytime)
```

1. **OmegaCore** finds freelance/automation jobs and executes them with local LLM
2. Earnings flow into the **WalletManager** and get allocated to trading
3. **TradeCore** trades on Polymarket using LLM Oracle probability estimates
4. Profits compound back → more jobs → more trading → self-accelerating
5. **MetaBrain** rewrites any underperforming strategy — fully self-improving
6. You inject money anytime to accelerate. Withdraw profits whenever you want.

---

## Architecture

| Module | Role |
|--------|------|
| `FlywheelBrain` | Master orchestrator — coordinates everything |
| `OmegaCore` | Finds & executes jobs → earns money |
| `TradeCore` | Trades capital on Polymarket → grows money |
| `WalletManager` | Capital allocation, auto-withdrawal, rebalancing |
| `MetaBrain` | HyperAgent — rewrites strategies that underperform |
| `MidasDB` | SQLite — tracks every dollar in/out/traded |
| `TelegramNotifier` | Real-time alerts + command interface |

---

## Setup (Termux)

```bash
# Install dependencies
pip install requests python-dotenv

# Clone
git clone https://github.com/kevinleestites2-dev/Midas-prime-
cd Midas-prime-

# Configure
cp .env.example .env
nano .env  # Add your tokens

# Run
python midas_prime.py
```

---

## .env Configuration

```env
# Telegram (required for notifications + commands)
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Ollama (local LLM)
OLLAMA_BASE=http://localhost:11434
OLLAMA_MODEL=qwen2.5-coder:7b

# Capital
INITIAL_CAPITAL=0.0
AUTO_WITHDRAW_THRESHOLD=500.0
AUTO_WITHDRAW_PCT=0.25

# Polymarket (optional)
POLY_API_KEY=your_key
POLY_SECRET=your_secret
```

---

## Telegram Commands

| Command | Description |
|---------|-------------|
| `/status` | Full system status |
| `/balance` | Current balance |
| `/deposit 100` | Add $100 to the flywheel |
| `/withdraw 50` | Withdraw $50 |
| `/earnings` | All-time stats |
| `/pause` | Pause the flywheel |
| `/resume` | Resume the flywheel |
| `/help` | Show all commands |

---

## The Pantheon

| Bot | Role |
|-----|------|
| **ZeusPrime** | King — autonomous AI agent OS |
| **OmegaPrime** | Worker — finds and executes tasks |
| **Open-trade** | Trader — Polymarket Meta trading bot |
| **MidasPrime** | Flywheel — fuses OmegaPrime + Open-trade into a self-funding machine |

---

*Built by kevinleestites2-dev | Part of the Pantheon*
