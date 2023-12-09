/// Population variance
pub fn compute_variance(values: Vec<f64>) -> f64 {
    let sum: f64 = values.iter().sum();
    let mean = sum / (values.len() as f64);
    let values_norm: Vec<f64> = values.iter().map(|&x| (x - mean).powi(2)).collect();
    let variance: f64 = values_norm.iter().sum::<f64>() / (values_norm.len() as f64);
    variance
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_variance() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let variance = compute_variance(values);
        assert_eq!(variance, 2.0);
    }
}
