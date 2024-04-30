// tests/processor_tests.rs
use chrono::{NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::US::Eastern;
use polyextract::{MarketTimezone, MinuteExtractor, PolyAggInfo, Processor};
use std::time::Instant;
use polyextract::processor::MarketHoursFilter;


#[tokio::test]
async fn test_timezone_datetime() {
    let date_str = "2024-01-02";
    let market_timezone = MarketTimezone::Eastern;

    let expected_start_datetime = NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let expected_end_datetime = NaiveDateTime::parse_from_str("2024-01-02 16:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    let expected_start_tz_datetime = Eastern.from_local_datetime(&expected_start_datetime).single().unwrap();
    let expected_end_tz_datetime = Eastern.from_local_datetime(&expected_end_datetime).single().unwrap();

    let (actual_start_tz_datetime, actual_end_tz_datetime) = market_timezone.market_hours_on_date(date_str).unwrap();

    assert_eq!(actual_start_tz_datetime, expected_start_tz_datetime);
    assert_eq!(actual_end_tz_datetime, expected_end_tz_datetime);
}

#[tokio::test]
async fn test_timezone_timestamps() {
    let date_str = "2024-01-02";
    let market_timezone = MarketTimezone::Eastern;

    let expected_start_timestamp = 1704205800000;
    let expected_end_timestamp = 1704229200000;

    let (actual_start_timestamp, actual_end_timestamp) = market_timezone.market_hours_on_date_millis(date_str).unwrap();

    assert_eq!(actual_start_timestamp, expected_start_timestamp);
    assert_eq!(actual_end_timestamp, expected_end_timestamp);
}


#[tokio::test]
async fn test_time_filter_single_day() {
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
    let df = minute_extractor.extractor.extract().await;

    // Create a mutable variable to hold the unwrapped DataFrame
    let unwrapped_df = df.unwrap();

    // Create a Processor with the DataFrame and MarketTimezone
    let market_timezone = MarketTimezone::Eastern;
    let processor = MarketHoursFilter::new(&unwrapped_df, &market_timezone, "2024-01-02");
    // Apply time filtering to the DataFrame
    let filtered_df = processor.filter().unwrap(); // Handle potential errors properly in real scenarios

    // Extract market hours to compare
    let (market_start, market_end) = market_timezone.market_hours_on_date_millis("2024-01-02").unwrap();

    // Verify that all rows in the DataFrame fall within the market hours
    let time_column = filtered_df.column("time").unwrap().i64().unwrap();
    assert!(time_column.into_iter().all(|opt_time| {
        opt_time.map(|time| time >= market_start && time <= market_end).unwrap_or(false)
    }));

    assert_eq!(filtered_df.height(), 391);

    let elapsed_time = start_time.elapsed().as_secs_f64(); // Calculate the elapsed time in seconds
    println!("Test execution time: {:.2} seconds", elapsed_time);
}


#[tokio::test]
async fn test_processor_single_day() {
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
    let df = minute_extractor.extractor.extract().await;

    // Create a mutable variable to hold the unwrapped DataFrame
    let mut unwrapped_df = df.unwrap();

    // Create a Processor with the DataFrame and MarketTimezone
    let market_timezone = MarketTimezone::Eastern;
    let mut processor = Processor::new(&mut unwrapped_df, &market_timezone);

    // Process the DataFrame
    let (p1_outliers, p2_outliers) = processor.process().unwrap();

    // Check the sum of outliers detected
    let total_outliers = p1_outliers + p2_outliers;
    assert_eq!(total_outliers, 4);

    // Check the length of the processed DataFrame
    assert_eq!(processor.df.height(), 391);

    let elapsed_time = start_time.elapsed().as_secs_f64(); // Calculate the elapsed time in seconds
    println!("Test execution time: {:.2} seconds", elapsed_time);
}

#[tokio::test]
async fn test_processor_multiple_days() {
    let start_time = Instant::now(); // Record the start time

    let start_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
    let poly_agg_info = PolyAggInfo {
        ticker: "AAPL".to_string(),
        start_date,
        end_date,
        resolution: "minute".to_string(),
        multiplier: 1,
    };

    let minute_extractor = MinuteExtractor::new(poly_agg_info);
    let df = minute_extractor.extractor.extract().await;

    // Create a mutable variable to hold the unwrapped DataFrame
    let mut unwrapped_df = df.unwrap();

    // Create a Processor with the DataFrame and MarketTimezone
    let market_timezone = MarketTimezone::Eastern;
    let mut processor = Processor::new(&mut unwrapped_df, &market_timezone);

    // Process the DataFrame
    let (p1_outliers, p2_outliers) = processor.process().unwrap();

    // Check the sum of outliers detected
    let total_outliers = p1_outliers + p2_outliers;
    assert_eq!(total_outliers, 7);

    // Check the length of the processed DataFrame
    assert_eq!(processor.df.height(), 782);

    let elapsed_time = start_time.elapsed().as_secs_f64(); // Calculate the elapsed time in seconds
    println!("Test execution time: {:.2} seconds", elapsed_time);
}