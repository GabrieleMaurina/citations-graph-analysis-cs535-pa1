from collections import defaultdict as dd, deque, Counter
from multiprocessing import Pool, cpu_count

citations = 'citations.txt'
dates = 'published-dates.txt'

ns = {}
es = dd(set)
ns_count = dd(int)
es_count = dd(int)

with open (dates, 'r') as nodes, open(citations, 'r') as edges:
    nodes = nodes.read()
    edges = edges.read()

nodes = ((int(n1), int(d.split('-')[0])) for n1, d in (n.split() for n in nodes.split('\n') if n and not n.startswith('#')) if len(n1) < 8)
edges = ((int(n1), int(n2)) for n1, n2 in (e.split() for e in edges.split('\n') if e and not e.startswith('#')))

for n, y in nodes:
    if n not in ns:
        ns_count[y] += 1
    ns[n] = y

for y in ns_count:
    if y-1 in ns_count:
        ns_count[y] += ns_count[y-1]

ns_count = tuple(sorted(ns_count.items()))
print('nodes per year:', ns_count)

for n1, n2 in edges:
    if n1 in ns and n2 in ns:
        es[n1].add(n2)
        es[n2].add(n1)
        es_count[ns[n1]] += 1

for y in es_count:
    if y-1 in es_count:
        es_count[y] += es_count[y-1]

es_count = tuple(sorted(es_count.items()))
print('edges per year:', es_count)

d90 = {}

for y in range(1992, 2003):
    c = Counter()
    g = {n:tuple(filter(lambda v: ns[v] <= y, e)) for n, e in es.items() if ns[n] <= y}
    t = len(g)

    def f(o):
        d = {o: 0}
        q = deque()
        q.append(o)
        while(q):
            n = q.popleft()
            for nn in g[n]:
                if nn not in d:
                    d[nn] = d[n] + 1
                    q.append(nn)
        m = dd(int)
        del d[o]
        for v in d.values():
           m[v] += 1
        return m

    with Pool(cpu_count()) as p:
        for i, m in enumerate(p.imap_unordered(f, g.keys())):
            c.update(m)
            if i%1000==0:
                print(y, f'{i/t*100:.2f}%')
    
    print('distances:', c)
    t = sum(c.values())
    s = 0
    for d, pd in zip(sorted(c.keys()), [0] + sorted(c.keys())):
        s += c[d]
        if s/t == 0.9:
            d90[y] = d
            break
        elif s/t > 0.9:
            u90 = (s-c[d]) / t
            o90 = s/t
            d90[y] = pd + (d-pd) * ((0.9-u90) / (o90-u90))
            break
    print('d90:', d90[y])

d90 = tuple(sorted(d90.items()))
print('diameters per year:', d90)
