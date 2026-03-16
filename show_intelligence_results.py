from political_data_store import PoliticalDataStore

store = PoliticalDataStore()
stats = store.get_stats()

print("=== DB STATS ===")
print("Total signals:", stats["total_signals"])
print("Enriched by GPT:", stats["enriched"])
print("Pending GPT:", stats["pending_enrichment"])
print("By source:", stats["by_source"])

print()
print("=== ENRICHED SIGNALS (sample) ===")
results = store.query_intelligence(days_back=90, only_enriched=True)
if not results:
    print("No enriched signals yet.")
else:
    for r in results[:10]:
        print("-" * 60)
        print("Member:    ", r["member_name"])
        print("Source:    ", r["source"])
        print("Title:     ", r["title"][:70])
        print("Sentiment: ", r["sentiment_score"])
        print("Tickers:   ", r["tickers_mentioned"])
        print("Industries:", r["industries_mentioned"])
        print("Posted:    ", r["posted_at"])

print()
print("=== ALL SIGNALS (last 90 days) ===")
all_r = store.query_intelligence(days_back=90, only_enriched=False)
print("Total rows:", len(all_r))
for r in all_r[:8]:
    tag = "GPT done" if r.get("sentiment_score") is not None else "PENDING"
    print("[" + tag + "]", r["member_name"], "|", r["source"], "|", r["title"][:55])
