use crate::structs::Edge;

pub fn bellman_ford_negative_cycle(n: usize, edges: &[Edge], source: usize) -> Option<Vec<usize>> {
    let mut dist = vec![std::f64::INFINITY; n];
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
    let mut visited = vec![false; n];
    visited[dest] = true;
    while path.len() <= n {
        if let Some(p) = pred[current] {
            current = p;
            if visited[p] {
                path = path.into_iter().rev().take_while(|&x| x != p).collect();
                path.push(p);
                break;
            }
            path.push(p);
            visited[p] = true;
        } else {
            break;
        }
    }
    if let Some(&first) = path.first() {
        path.push(first);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bellman_ford_negative_cycle() {
        let edges = vec![
            Edge {
                src: 0,
                dest: 1,
                weight: 1.0,
            },
            Edge {
                src: 1,
                dest: 2,
                weight: 1.0,
            },
            Edge {
                src: 2,
                dest: 3,
                weight: -4.0,
            },
            Edge {
                src: 3,
                dest: 1,
                weight: 2.0,
            },
            Edge {
                src: 1,
                dest: 4,
                weight: 1.0,
            },
            Edge {
                src: 3,
                dest: 4,
                weight: 3.0,
            },
        ];

        let mut result = bellman_ford_negative_cycle(5, &edges, 1);
        // Ensure the starting position is 2 to find the same cycle as the test-case
        if let Some(ref mut path) = result {
            path.pop();
            if let Some(position) = path.iter().position(|&x| x == 2) {
                path.rotate_left(position);
            }
            path.push(*path.first().unwrap());
        }
        assert_eq!(result, Some(vec![2, 3, 1, 2]));
    }
}
