"""Diagnostic: inspect what GFW search_vessels actually returns."""
import asyncio
import os
import sys
import gfwapiclient as gfw


async def main(query: str) -> None:
    client = gfw.Client(access_token=os.getenv("GFW_API_KEY"))
    print(f"Querying GFW for: {query!r}")
    result = await client.vessels.search_vessels(
        query=query,
        datasets=["public-global-vessel-identity:latest"],
        limit=3,
    )
    print(f"RESULT TYPE: {type(result).__name__}")
    print(f"RESULT DIR: {[x for x in dir(result) if not x.startswith('_')]}")

    try:
        d = result.data()
        print(f"DATA TYPE: {type(d).__name__}")
        if hasattr(d, "__iter__") and not isinstance(d, (str, dict)):
            items = list(d)
            print(f"DATA LENGTH: {len(items)}")
            if items:
                first = items[0]
                print(f"FIRST ITEM TYPE: {type(first).__name__}")
                if hasattr(first, "model_dump"):
                    dumped = first.model_dump()
                    print(f"FIRST.model_dump() KEYS: {list(dumped.keys())[:20]}")
                    import json
                    print("FIRST (truncated):")
                    print(json.dumps(dumped, default=str, indent=2)[:2000])
                else:
                    print(f"FIRST REPR: {repr(first)[:500]}")
        else:
            print(f"DATA REPR: {repr(d)[:500]}")
    except Exception as e:
        print(f"data() error: {type(e).__name__}: {e}")


if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "311000720"
    asyncio.run(main(query))
