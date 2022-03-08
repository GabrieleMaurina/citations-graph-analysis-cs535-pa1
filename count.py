from collections import defaultdict as dd

published_dates = 'published-dates.txt'
citations = 'citations.txt'

with open(published_dates, 'r') as ids:
    ids = (line.split() for line in ids.read().split('\n') if line and not line.startswith('#'))

with open(citations, 'r') as edges:
    edges = (id for line in edges.read().split('\n') for id in line.split() if line and not line.startswith('#'))

#ids = edges

lengths = dd(int)
s11lengths = dd(int)
s11values = dd(set)
for id, year in ids:
    lengths[len(id)] += 1
    if id.startswith('11'):
        s11lengths[len(id)] += 1
        s11values[len(id)].add((id, year))

print(lengths, s11lengths)
for length, ids in s11values.items():
    print(length)
    for id in ids:
        print(id)
    print()
