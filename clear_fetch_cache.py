import sqlite3
from config import POLITICAL_INTELLIGENCE_DB

with sqlite3.connect(POLITICAL_INTELLIGENCE_DB) as conn:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print("Tables:", [t[0] for t in tables])
    for t in tables:
        name = t[0]
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name}: {count} rows")
        if "run" in name or "log" in name:
            conn.execute(f"DELETE FROM {name}")
            conn.commit()
            print(f"  -> Cleared {name}")
print("Cache cleared.")
