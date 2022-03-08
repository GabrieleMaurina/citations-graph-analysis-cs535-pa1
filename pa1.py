from pyspark import SparkContext
from collections import deque
from collections import Counter

CITATIONS = '/user/input/citations.txt'
PUBLISHED_DATES = '/user/input/published-dates.txt'

CONNECTED_NODES = {1992: 727, 1993: 869493, 1994: 6685253, 1995: 22234693, 1996: 50406009, 1997: 76823404}
DIAMETER_THRESHOLD = 0.9

def read_files(sc):
    return sc.textFile(CITATIONS), sc.textFile(PUBLISHED_DATES)

def map_citations(line):
    values = line.split('\t',1)
    return int(values[0]), int(values[1])

def map_published_dates(line):
    values = line.split('\t',1)
    id = values[0]
    if id.startswith('11'):
        id = id[2:]
    id = int(id)
    year = int(values[1].split('-', 1)[0])
    return id, year

def clean_data(citations, published_dates):
    clean_citations = citations.filter(lambda v: not v.startswith('#')).map(map_citations).filter(lambda v: v[0]!=v[1]).distinct()
    clean_published_dates = published_dates.filter(lambda v: not v.startswith('#')).map(map_published_dates).reduceByKey(min)
    return clean_citations, clean_published_dates

def get_diameter_from_distances(distances, threshold, total=None):
    threshold = max(0, min(1, threshold))
    cumulative_distances = []
    tot = 0
    for distance, count in distances:
        tot += count
        cumulative_distances.append([distance, count, tot])
    if total: tot = total
    for values in cumulative_distances:
        values.append(values[2]/tot)
    pred = 0, 0
    for distance, count, cumulative_count, percentage in cumulative_distances:
        if percentage >= threshold:
            return pred[0] + (distance-pred[0]) * ((0.9-pred[1]) / (percentage-pred[1]))
        else:
            pred = distance, percentage

def get_distances_with_bfs(id, g):
    d = {id: 0}
    q = deque()
    q.append(id)
    while(q):
        n = q.popleft()
        for nn in g[n]:
            if nn not in d:
                d[nn] = d[n] + 1
                q.append(nn)
    del d[id]
    return Counter(d.values()).items()


def get_diameter_with_bfs(nodes, edges, threshold):
    edges = edges.union(edges.map(lambda v: (v[1],v[0]))).distinct()
    g = edges.groupByKey().map(lambda v: (v[0], tuple(v[1]))).union(nodes.subtractByKey(edges).map(lambda v: (v[0], ()))).collectAsMap()
    def get_dist(v):
        return get_distances_with_bfs(v[0], g)
    distances = nodes.flatMap(get_dist).reduceByKey(lambda v1, v2: v1+v2).collect()
    return get_diameter_from_distances(distances, threshold)

def pair(*v):
    if len(v) == 1:
        v = v[0]
    return min(v), max(v)

def adj_to_pairs(ids, d):
    return tuple((pair(id1, id2), d) for id1 in ids for id2 in ids if id1 != id2)


def get_diameter(nodes, edges, threshold, total):
    edges = edges.union(edges.map(lambda v: (v[1],v[0]))).distinct()
    dist_1 = edges.map(lambda v: (pair(v), 1)).reduceByKey(min)
    dist_1_first = dist_1.map(lambda v: (v[0][0], v[0][1]))
    dist_1_second = dist_1.map(lambda v: (v[0][1], v[0][0]))
    dist_1_pairs = dist_1_first.union(dist_1_second).cache()
    distances = dist_1.union(edges.groupByKey().flatMap(lambda v: adj_to_pairs(v[1], 2))).reduceByKey(min).cache()
    last_dist = distances.filter(lambda v: v[1]==2).cache()
    covered = distances.count()
    for d in range(3,21):
        print(d, last_dist.count())
        last_dist_first = last_dist.map(lambda v: (v[0][0], v[0][1]))
        last_dist_second = last_dist.map(lambda v: (v[0][1], v[0][0]))
        last_dist_pairs = last_dist_first.union(last_dist_second)
        new_dist = dist_1_pairs.join(last_dist_pairs).filter(lambda v: v[0]!=v[1]).map(lambda v: (pair(v[1]), d))
        last_dist.unpersist()
        print(new_dist.count())
        updated_distances = distances.union(new_dist).reduceByKey(min).cache()
        distances.unpersist()
        distances = updated_distances
        last_dist = distances.filter(lambda v: v[1]==d).cache()
        last_covered = last_dist.count()
        covered += last_covered
        if not last_covered or covered/total > threshold:
            break
    dist_count = distances.map(lambda v: (v[1], 1)).reduceByKey(lambda v1, v2: v1+v2).collect()
    return get_diameter_from_distances(dist_count, threshold, total)

def compute_year(year, clean_citations, clean_published_dates):
    nodes = clean_published_dates.filter(lambda v: v[1]<=year)
    edges = clean_citations.join(nodes).map(lambda v: (v[1][0], v[0])).join(nodes).map(lambda v: (v[1][0],v[0]))
    node_count = nodes.count()
    edge_count = edges.count()
    diameter = get_diameter_with_bfs(nodes, edges, DIAMETER_THRESHOLD)
    #diameter = get_diameter(nodes, edges, DIAMETER_THRESHOLD, CONNECTED_NODES[year]) if year < 1998 else 0
    values = (year, node_count, edge_count, diameter)
    print(','.join(str(v) for v in values))
    return values

sc = None

def main():
    global sc
    sc = SparkContext('local', 'PA1')
    sc.setLogLevel('ERROR')
    citations, published_dates = read_files(sc)
    clean_citations, clean_published_dates = clean_data(citations, published_dates)
    data = tuple(compute_year(year, clean_citations, clean_published_dates) for year in range(1992, 2003))

if __name__ == '__main__':
    main()
