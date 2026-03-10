"""
GolfGen Amazon Pipeline Scheduler
===================================
Runs data pulls on schedule and regenerates the dashboard.

Usage:
    python scripts/scheduler.py              # One-time pull + dashboard refresh
    python scripts/scheduler.py --loop       # Run continuously (hourly pulls)
    python scripts/scheduler.py --backfill 90  # Backfill last 90 days
"""

import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


def run_pull(days_back=7):
    """Run the SP-API data pull."""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Starting data pull ({days_back} days)...")
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "amazon_sp_api.py"), str(days_back)],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERRORS:\n{result.stderr}")
    return result.returncode == 0


def generate_dashboard():
    """Regenerate the HTML dashboard."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Regenerating dashboard...")
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_dashboard.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    return result.returncode == 0


def main():
    if "--backfill" in sys.argv:
        idx = sys.argv.index("--backfill")
        days = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 90
        print(f"Backfilling {days} days of data...")
        run_pull(days)
        generate_dashboard()
        return

    if "--loop" in sys.argv:
        print("Starting hourly pull loop. Press Ctrl+C to stop.")
        while True:
            success = run_pull(2)  # Overlap by 2 days to catch late-arriving data
            if success:
                generate_dashboard()
            else:
                print("Pull failed. Will retry next cycle.")
            print(f"Next pull at {datetime.now().strftime('%H:%M')} + 1 hour. Sleeping...")
            time.sleep(3600)
    else:
        # One-time run
        run_pull(7)
        generate_dashboard()


if __name__ == "__main__":
    main()
