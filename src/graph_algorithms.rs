const INF: f64 = std::f64::INFINITY;

pub struct Edge {
    pub src: usize,
    pub dest: usize,
    pub weight: f64,
}

pub fn bellman_ford_negative_cycle(n: usize, edges: &[Edge], source: usize) -> Option<Vec<usize>> {
    let mut dist = vec![INF; n];
    let mut pred = vec![None; n];
    dist[source] = 0.0;

    for _ in 0..n - 1 {
        for edge in edges {
            if dist[edge.src] + edge.weight < dist[edge.dest] {
                dist[edge.dest] = dist[edge.src] + edge.weight;
                pred[edge.dest] = Some(edge.src);
            }
        }
    }

    for edge in edges {
        if dist[edge.src] + edge.weight < dist[edge.dest] {
            let path = backtrack_negative_cycle_path(n, &pred, edge.dest);
            return Some(path);
        }
    }

    None
}

fn backtrack_negative_cycle_path(n: usize, pred: &[Option<usize>], dest: usize) -> Vec<usize> {
    let mut path = vec![dest];
    let mut current = dest;
    while path.len() <= n && (path.len() == 1 || current != dest) {
        if let Some(p) = pred[current] {
            path.push(p);
            current = p;
        } else {
            break;
        }
    }
    path.reverse();
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bellman_ford_negative_cycle() {
        let edges = vec![
            Edge { src: 0, dest: 1, weight: 1.0 },
            Edge { src: 1, dest: 2, weight: 1.0 },
            Edge { src: 2, dest: 3, weight: -4.0 },
            Edge { src: 3, dest: 1, weight: 2.0 },
            Edge { src: 1, dest: 4, weight: 1.0 },
            Edge { src: 3, dest: 4, weight: 3.0 }
        ];
        let result = bellman_ford_negative_cycle(5, &edges, 1);
        assert_eq!(result, Some(vec![2, 3, 1, 2]));
    }
}