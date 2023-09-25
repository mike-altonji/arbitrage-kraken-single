use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::time::Duration;
use crate::graph_algorithms::floyd_warshall_fast;

const INF: f64 = std::f64::INFINITY;
const FEE: f64 = 0.0026;

pub async fn evaluate_arbitrage_opportunities(pair_to_assets: HashMap<String, (String, String)>, shared_asset_pairs: Arc<Mutex<HashMap<String, (f64, f64)>>>) {
    tokio::time::sleep(Duration::from_secs(3)).await;  // Give shared_asset_pairs time to populate
    loop {
        let start_time = Instant::now();
        let asset_pairs = shared_asset_pairs.lock().unwrap().clone();
        let (n, mut dist) = prepare_graph(&asset_pairs, &pair_to_assets);
        floyd_warshall_fast(&mut dist);
        let node = detect_negative_cycles(&dist, n);
        let duration = start_time.elapsed();
        if node.is_some() {
            println!("!!! ARBITRAGE OPPORTUNITY AT NODE {:?}. # Nodes: {}, Time: {:?}", node.unwrap(), n, duration);
        } else {
            println!("# Nodes: {}, # Edges: {}, Time: {:?}", n, asset_pairs.len(), duration);
        }
    }
}


fn prepare_graph(asset_pairs: &HashMap<String, (f64, f64)>, pair_to_assets: &HashMap<String, (String, String)>) -> (usize, Vec<Vec<f64>>) {
    let mut asset_to_index = HashMap::new();
    let mut index = 0;
    let mut edges = vec![];

    for (pair, (bid, ask)) in asset_pairs {
        if let Some((asset1, asset2)) = pair_to_assets.get(pair) {
            let index1 = *asset_to_index.entry(asset1.clone()).or_insert_with(|| { index += 1; index - 1 });
            let index2 = *asset_to_index.entry(asset2.clone()).or_insert_with(|| { index += 1; index - 1 });
            edges.push((index1, index2, bid * (1.0 - FEE)));
            edges.push((index2, index1, 1.0 / (ask * (1.0 + FEE))));
        }
    }

    let mut dist = vec![vec![INF; index]; index];
    for &(i, j, w) in &edges {
        dist[i][j] = w;
    }

    (index, dist)
}


fn detect_negative_cycles(dist: &[Vec<f64>], n: usize) -> Option<usize> {
    for i in 0..n {
        if dist[i][i] < 0.0 {
            return Some(i);
        }
    }
    None
}
