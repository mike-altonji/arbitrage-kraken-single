use log4rs::{append::file::FileAppender, config};
use std::time::{SystemTime, UNIX_EPOCH};

/// Initialize logging. Will create a file in `logs/arbitrage_log_{timestamp}.log`
pub fn init_logging() {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time invalid");
    let timestamp = since_the_epoch.as_secs();
    let log_config = FileAppender::builder()
        .build(format!("logs/arbitrage_log_{}.log", timestamp))
        .expect("Unable to build log file");
    let log_config = config::Config::builder()
        .appender(config::Appender::builder().build("default", Box::new(log_config)))
        .build(
            config::Root::builder()
                .appender("default")
                .build(log::LevelFilter::Info),
        )
        .expect("Unable to build log file");
    log4rs::init_config(log_config).expect("Unable to build log file");
}

/// Helper function to list CSV files in a directory
pub fn get_csv_files_from_directory(
    directory: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let paths = std::fs::read_dir(directory)?;
    let csv_files: Vec<_> = paths
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(std::ffi::OsStr::to_str) == Some("csv"))
        .map(|e| e.path().to_str().unwrap().to_string())
        .collect();
    Ok(csv_files)
}

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
