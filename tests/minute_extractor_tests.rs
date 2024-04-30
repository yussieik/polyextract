// tests/minute_extractor_tests.rs
use chrono::NaiveDate;
use polyextract::MinuteExtractor;
use polyextract::PolyAggInfo;
use std::time::Instant;


#[tokio::test]
async fn test_minute_extractor() {
    let start_time = Instant::now(); // Record the start time

    let start_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let poly_agg_info = PolyAggInfo {
        ticker: "AAPL".to_string(),
        start_date,
        end_date,
        resolution: "minute".to_string(),
        multiplier: 1,
    };

    let minute_extractor = MinuteExtractor::new(poly_agg_info);
    let result = minute_extractor.extractor.extract().await;

    match result {
        Ok(df) => {
            println!("Successfully Extracted DataFrame:\n{}", df);
            // Add assertions to validate the DataFrame
        }
        Err(error) => {
            eprintln!("Error: {}", error);
            panic!("Failed to extract data");
        }
    }

    let elapsed_time = start_time.elapsed().as_secs_f64(); // Calculate the elapsed time in seconds
    println!("Test execution time: {:.2} seconds", elapsed_time);
}

#[tokio::test]
async fn test_minute_extractor_concurrent_multiple_tickers() {
    let start_time = Instant::now();

    let tickers = vec!["GOOGL", "AAPL", "MSFT", "TSLA", "NVDA"];
    let start_date = NaiveDate::from_ymd_opt(2007, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

    let futures = tickers.into_iter().map(|ticker| {
        let poly_agg_info = PolyAggInfo {
            ticker: ticker.to_string(),
            start_date,
            end_date,
            resolution: "minute".to_string(),
            multiplier: 1,
        };

        let minute_extractor = MinuteExtractor::new(poly_agg_info);
        async move {
            minute_extractor.extractor.extract().await
        }
    }).collect::<Vec<_>>();

    let results = futures::future::join_all(futures).await;

    for result in results {
        match result {
            Ok(df) => {
                println!("Successfully Extracted DataFrame:\n{}", df);
                // Add assertions to validate the DataFrame
            }
            Err(error) => {
                eprintln!("Error: {}", error);
            }
        }
    }

    let elapsed_time = start_time.elapsed().as_secs_f64();
    println!("Test execution time: {:.2} seconds", elapsed_time);
}