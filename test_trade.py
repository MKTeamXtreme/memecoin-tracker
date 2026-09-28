import asyncio
import time
from autotrader import on_new_coin

async def run_test():
    print("========================================")
    print("  SIMULATING A 100-SCORE COIN DETECTION ")
    print("========================================")
    
    # We pass in a fake coin that immediately passes the 1-minute test
    await on_new_coin(
        session=None,
        mint="TEST_" + str(int(time.time())), # unique mint so it doesn't collide
        name="BugFix Tester Coin",
        entry_mc=50000.0,
        mc_1min=60000.0 # Force a 1.2x pump to guarantee it buys
    )
    
    print("\nTest finished! If you see 'BugFix Tester Coin' in your Paper Trades tab, the bug is completely fixed!")

if __name__ == "__main__":
    asyncio.run(run_test())
