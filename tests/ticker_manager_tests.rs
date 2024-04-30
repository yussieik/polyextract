// tests/ticker_manager_tests.rs

use chrono::NaiveDate;
use polyextract::poly_agg_info::PolyAggInfo;
use polyextract::TickerManager;
use std::time::Instant;

#[tokio::test]
async fn test_ticker_manager() {
    let start_time = Instant::now();

    let poly_agg_info = PolyAggInfo {
        ticker: "AAPL".to_string(),
        start_date: NaiveDate::from_ymd_opt(2014, 1, 1).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        resolution: "minute".to_string(),
        multiplier: 1,
    };

    let ticker_manager = TickerManager::new(poly_agg_info);
    let result = ticker_manager.process_data().await;

    match result {
        Ok(df) => {
            println!("Successfully Processed DataFrame:\n{}", df);
            // Add assertions to validate the DataFrame
        }
        Err(error) => {
            eprintln!("Error: {}", error);
            panic!("Failed to process data");
        }
    }

    let elapsed_time = start_time.elapsed().as_secs_f64();
    println!("Test execution time: {:.2} seconds", elapsed_time);
}