"""
Run intelligence_enricher repeatedly until all pending signals are enriched.
Processes 50 at a time (API rate limit safety).
"""
import subprocess
import sys

from political_data_store import PoliticalDataStore

store = PoliticalDataStore()

while True:
    stats = store.get_stats()
    pending = stats["pending_enrichment"]
    print(f"Pending: {pending}  |  Enriched: {stats['enriched']}  |  Total: {stats['total_signals']}")
    if pending == 0:
        print("All signals enriched!")
        break
    result = subprocess.run(
        [sys.executable, "intelligence_enricher.py"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    # Print only non-empty lines to reduce noise
    for line in result.stdout.splitlines():
        if line.strip():
            try:
                print(" ", line)
            except UnicodeEncodeError:
                print(" ", line.encode("ascii", errors="replace").decode("ascii"))
    if result.returncode != 0:
        print("Enricher exited with error.")
        break

print("Done.")
store2 = PoliticalDataStore()
final = store2.get_stats()
print(f"Final: {final['enriched']} enriched / {final['total_signals']} total")
