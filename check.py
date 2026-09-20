import time, urllib.request, json
time.sleep(20)
r = json.loads(urllib.request.urlopen('http://localhost:8000/api/coins').read())
print(f'Coins in DB: {len(r)}')
for c in r[:8]:
    print(f"  {c['name']} ${c['symbol']}  score={c['score']}  mc=${c['initial_mc']:.0f}")
