const INF: f64 = std::f64::INFINITY;

pub struct Edge {
    pub src: usize,
    pub dest: usize,
    pub weight: f64,
}

pub fn floyd_warshall_fast(dist: &mut [Vec<f64>]) {
    let n = dist.len();
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            let (dist_j, dist_i) = if j < i {
                let (lo, hi) = dist.split_at_mut(i);
                (&mut lo[j][..n], &mut hi[0][..n])
            } else {
                let (lo, hi) = dist.split_at_mut(j);
                (&mut hi[0][..n], &mut lo[i][..n])
            };
            let dist_ji = dist_j[i];
            for k in 0..n {
                dist_j[k] = f64::min(dist_j[k], dist_ji + dist_i[k]);
            }
        }
    }
}

pub fn bellman_ford_negative_cycle(n: usize, edges: &[Edge], source: usize) -> Option<usize> {
    let mut dist = vec![INF; n];
    dist[source] = 0.0;

    for _ in 0..n - 1 {
        for edge in edges {
            if dist[edge.src] + edge.weight < dist[edge.dest] {
                dist[edge.dest] = dist[edge.src] + edge.weight;
            }
        }
    }

    for edge in edges {
        if dist[edge.src] + edge.weight < dist[edge.dest] {
            return Some(edge.dest);
        }
    }

    None
}