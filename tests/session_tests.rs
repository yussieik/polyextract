use chrono::prelude::*;
use futures::future::join_all;
use polyextract::PolygonHistorySession;
use std::time::Instant;

#[tokio::test]
async fn test_polygon_api_minute_request() {
    let url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2023-01-09/2023-01-09?adjusted=true&sort=asc&limit=5000";
    let response = PolygonHistorySession::send_request(url).await;

    match response {
        Ok(res) => {
            let body = res.text().await.unwrap();
            println!("Response body: {}", body);
            // Assert the expected behavior or validate the response data
            assert!(body.contains("ticker"));
            assert!(body.contains("AAPL"));
            // Add more assertions as needed
        }
        Err(error) => {
            eprintln!("Error: {}", error);
        }
    }
}

#[tokio::test]
async fn test_polygon_api_day_request() {
    let url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-09/2023-01-09?adjusted=true&sort=asc&limit=120";
    let response = PolygonHistorySession::send_request(url).await;

    match response {
        Ok(res) => {
            let body = res.text().await.unwrap();
            println!("Response body: {}", body);
            // Assert the expected behavior or validate the response data
            assert!(body.contains("ticker"));
            assert!(body.contains("AAPL"));
            // Add more assertions as needed
        }
        Err(error) => {
            eprintln!("Error: {}", error);
        }
    }
}

#[tokio::test]
async fn test_polygon_api_week_request() {
    let url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/week/2023-01-01/2023-01-09?adjusted=true&sort=asc&limit=120";
    let response = PolygonHistorySession::send_request(url).await;

    match response {
        Ok(res) => {
            let body = res.text().await.unwrap();
            println!("Response body: {}", body);
            // Assert the expected behavior or validate the response data
            assert!(body.contains("ticker"));
            assert!(body.contains("AAPL"));
            // Add more assertions as needed
        }
        Err(error) => {
            eprintln!("Error: {}", error);
        }
    }
}

#[tokio::test]
async fn test_polygon_api_month_request() {
    let url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/month/2023-01-01/2023-02-01?adjusted=true&sort=asc&limit=120";
    let response = PolygonHistorySession::send_request(url).await;

    match response {
        Ok(res) => {
            let body = res.text().await.unwrap();
            println!("Response body: {}", body);
            // Assert the expected behavior or validate the response data
            assert!(body.contains("ticker"));
            assert!(body.contains("AAPL"));
            // Add more assertions as needed
        }
        Err(error) => {
            eprintln!("Error: {}", error);
        }
    }
}

#[tokio::test]
async fn test_polygon_api_concurrent_requests() {
    let tickers = vec!["AAPL", "GOOGL", "MSFT"];
    let start_date = "2023-01-09";
    let end_date = "2023-01-09";

    let futures = tickers.into_iter().map(|ticker| {
        let url = format!("https://api.polygon.io/v2/aggs/ticker/{}/range/1/minute/{}/{}?adjusted=true&sort=asc&limit=5000", ticker, start_date, end_date);
        tokio::spawn(async move {
            let response = PolygonHistorySession::send_request(&url).await;
            (ticker, response)
        })
    });

    let results = join_all(futures).await;

    for result in results {
        match result {
            Ok((ticker, Ok(res))) => {
                let body = res.text().await.unwrap();
                println!("Response body for {}: {}", ticker, body);
                // Assert the expected behavior or validate the response data
                assert!(body.contains("ticker"));
                assert!(body.contains(ticker));
                // Add more assertions as needed
            }
            Ok((ticker, Err(error))) => {
                eprintln!("Error for {}: {}", ticker, error);
            }
            Err(error) => {
                eprintln!("Join error: {:?}", error);
            }
        }
    }
}

#[tokio::test]
async fn test_polygon_api_concurrent_single_ticker_requests() {
    let ticker = "AAPL";
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end_date = Utc.with_ymd_and_hms(2023, 5, 1, 0, 0, 0).unwrap();

    let date_range = (0..((end_date - start_date).num_days()))
        .map(|days| start_date + chrono::Duration::days(days))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .collect::<Vec<_>>();

    let futures = date_range.into_iter().map(|date| {
        let url = format!("https://api.polygon.io/v2/aggs/ticker/{}/range/1/minute/{}/{}?adjusted=true&sort=asc&limit=5000", ticker, date, date);
        tokio::spawn(async move {
            let response = PolygonHistorySession::send_request(&url).await;
            (date, response)
        })
    });

    let results = join_all(futures).await;

    for result in results {
        match result {
            Ok((_date, Ok(res))) => {
                let body = res.text().await.unwrap();
                // println!("Response body for {} on {}: {}", ticker, date, body);
                // Assert the expected behavior or validate the response data
                assert!(body.contains("ticker"));
                assert!(body.contains(ticker));
                // Add more assertions as needed
            }
            Ok((_date, Err(error))) => {
                eprintln!("Error for {} on {}: {}", ticker, _date, error);
            }
            Err(error) => {
                eprintln!("Join error: {:?}", error);
            }
        }
    }
}

#[tokio::test]
async fn test_polygon_api_concurrent_multiple_tickers_requests() {
    let tickers = vec!["AAPL", "GOOGL", "MSFT"];
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end_date = Utc.with_ymd_and_hms(2023, 5, 1, 0, 0, 0).unwrap();

    let date_range = (0..((end_date - start_date).num_days()))
        .map(|days| start_date + chrono::Duration::days(days))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .collect::<Vec<_>>();

    let futures = tickers.into_iter().flat_map(|ticker| {
        let date_range = date_range.clone();
        date_range.into_iter().map(move |date| {
            let url = format!("https://api.polygon.io/v2/aggs/ticker/{}/range/1/minute/{}/{}?adjusted=true&sort=asc&limit=5000", ticker, date, date);
            tokio::spawn(async move {
                let response = PolygonHistorySession::send_request(&url).await;
                (ticker, date, response)
            })
        })
    });

    let results = join_all(futures).await;

    for result in results {
        match result {
            Ok((ticker, _date, Ok(res))) => {
                let body = res.text().await.unwrap();
                // println!("Response body for {} on {}: {}", ticker, date, body);
                // Assert the expected behavior or validate the response data
                assert!(body.contains("ticker"));
                assert!(body.contains(ticker));
                // Add more assertions as needed
            }
            Ok((ticker, date, Err(error))) => {
                eprintln!("Error for {} on {}: {}", ticker, date, error);
            }
            Err(error) => {
                eprintln!("Join error: {:?}", error);
            }
        }
    }
}

#[tokio::test]
async fn test_rate_limiter() {
    let num_requests = 500;
    let expected_time = num_requests as f32 / 100.0;

    let start_time = Instant::now();

    let futures = (0..num_requests).map(|_| {
        let url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2023-01-01/2023-01-01?adjusted=true&sort=asc&limit=1";
        tokio::spawn(async move {
            let response = PolygonHistorySession::send_request(url).await;
            response
        })
    });

    let results = join_all(futures).await;

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time).as_secs_f32();

    println!("Elapsed time: {:.2} seconds", elapsed_time);
    println!("Expected time: {:.2} seconds", expected_time);

    assert!((elapsed_time - expected_time).abs() < 1.0);

    for result in results {
        match result {
            Ok(Ok(res)) => {
                let status = res.status();
                assert_eq!(status, reqwest::StatusCode::OK);
            }
            Ok(Err(error)) => {
                eprintln!("Error: {}", error);
                panic!("Request failed");
            }
            Err(error) => {
                eprintln!("Join error: {:?}", error);
                panic!("Join failed");
            }
        }
    }
}