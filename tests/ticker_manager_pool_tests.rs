// tests/ticker_manager_pool_tests.rs

use chrono::NaiveDate;
use polyextract::poly_agg_info::PolyAggInfo;
use polyextract::TickerManagerPool;
use std::time::Instant;

#[tokio::test]
async fn test_ticker_manager_pool() {
    let start_time = Instant::now();

    let tickers = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()];
    let start_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let resolution = "minute".to_string();
    let multiplier = 1;

    let poly_agg_infos = PolyAggInfo::create_poly_agg_infos(
        tickers,
        start_date,
        end_date,
        resolution,
        multiplier,
    );

    let ticker_manager_pool = TickerManagerPool::new(poly_agg_infos);
    let results = ticker_manager_pool.process_data_concurrently().await;

    match results {
        Ok(dataframes) => {
            assert_eq!(dataframes.len(), 3);

            for df in dataframes {
                println!("Successfully Processed DataFrame:\n{}", df);

                // Add assertions to validate the DataFrame
                assert_eq!(df.column("mkt_date").unwrap().str().unwrap().get(0).unwrap(), "2024-01-02");
                assert_eq!(df.column("open").unwrap().f64().unwrap().get(0).unwrap() > 0.0, true);
                assert_eq!(df.column("high").unwrap().f64().unwrap().get(0).unwrap() > 0.0, true);
                assert_eq!(df.column("low").unwrap().f64().unwrap().get(0).unwrap() > 0.0, true);
                assert_eq!(df.column("close").unwrap().f64().unwrap().get(0).unwrap() > 0.0, true);
                assert_eq!(df.column("volume").unwrap().i64().unwrap().get(0).unwrap() > 0, true);
                assert_eq!(df.column("vwap").unwrap().f64().unwrap().get(0).unwrap() > 0.0, true);
                assert_eq!(df.column("transactions").unwrap().i64().unwrap().get(0).unwrap() > 0, true);
            }
        }
        Err(error) => {
            eprintln!("Error: {}", error);
            panic!("Failed to process data");
        }
    }

    let elapsed_time = start_time.elapsed().as_secs_f64();
    println!("Test execution time: {:.2} seconds", elapsed_time);
}


#[tokio::test]
async fn test_ticker_manager_pool_17_yrs() {
    let start_time = Instant::now();

    let tickers = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string(), "TSLA".to_string(), "NVDA".to_string()];
    let start_date = NaiveDate::from_ymd_opt(2007, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 4, 26).unwrap();
    let resolution = "minute".to_string();
    let multiplier = 1;

    let poly_agg_infos = PolyAggInfo::create_poly_agg_infos(
        tickers,
        start_date,
        end_date,
        resolution,
        multiplier,
    );

    let ticker_manager_pool = TickerManagerPool::new(poly_agg_infos);
    let results = ticker_manager_pool.process_data_concurrently().await;

    match results {
        Ok(dataframes) => {
            assert_eq!(dataframes.len(), 5);

            for df in dataframes {
                println!("Successfully Processed DataFrame:\n{}", df);
            }
        }
        Err(error) => {
            eprintln!("Error: {}", error);
            panic!("Failed to process data");
        }
    }

    let elapsed_time = start_time.elapsed().as_secs_f64();
    println!("Test execution time: {:.2} seconds", elapsed_time);
}