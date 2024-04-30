// src/data_extractor.rs

use super::poly_agg_info::PolyAggInfo;
use super::PolygonHistorySession;
use chrono::Duration as ChronoDuration;
use futures::future::join_all;
use polars::prelude::*;
use serde_json::{to_string, Value};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use futures::StreamExt;
use tokio::time::{Instant, sleep};

/// Represents an aggregate data extractor for retrieving data from the Polygon API.
pub struct AggDataExtractor {
    pub poly_agg_info: PolyAggInfo,
    pub base_query: String,
    pub limit: String,
}

impl AggDataExtractor{
    /// Extracts aggregate data from the Polygon API based on the provided information.
    ///
    /// Returns a DataFrame containing the extracted aggregate data.
    pub async fn extract(&self) -> Result<DataFrame, PolarsError> {
        let date_range = DateRangeBuilder::create(&self.poly_agg_info);
        let queries = QueryBuilder::build(&self.base_query, &self.poly_agg_info, &self.limit, &date_range);

        let agg_data_schema = DataFrameBuilder::create_schema();
        let mut combined_df = DataFrameBuilder::create_empty();

        let mut failed_requests = Vec::new();

        let mut response_stream = futures::stream::iter(queries)
            .map(|query| {
                let date = extract_date(&query);
                async move {
                    match PolygonHistorySession::send_request(&query).await {
                        Ok(response) => (date, Ok(response)),
                        Err(error) => (query, Err(error.to_string())),
                    }
                }
            })
            .buffer_unordered(100);

        while let Some(result) = response_stream.next().await {
            match result {
                (date, Ok(response)) => {
                    if let Some(df) = ResponseProcessor::process_single((date, Ok(response)), &agg_data_schema).await? {
                        DataFrameBuilder::combine(&mut combined_df, vec![df])?;
                    }
                }
                (query, Err(error)) => {
                    failed_requests.push((query, error));
                }
            }
        }

        RequestSender::retry_failed(&failed_requests, &agg_data_schema, &mut combined_df).await?;

        DataFrameBuilder::finalize(&mut combined_df, &self.poly_agg_info.ticker)?;

        Ok(combined_df)
    }
}

/// Builds a date range based on the provided PolyAggInfo.
struct DateRangeBuilder;

impl DateRangeBuilder {
    /// Creates a vector of date strings based on the provided PolyAggInfo.
    fn create(poly_agg_info: &PolyAggInfo) -> Vec<String> {
        let mut date_range = Vec::new();
        let mut current_date = poly_agg_info.start_date;
        while current_date <= poly_agg_info.end_date {
            date_range.push(current_date.format("%Y-%m-%d").to_string());
            current_date += ChronoDuration::days(1);
        }
        date_range
    }
}

/// Builds queries based on the provided base query, PolyAggInfo, limit, and date range.
struct QueryBuilder;

impl QueryBuilder {
    /// Builds a vector of query strings by replacing placeholders in the base query
    /// with the corresponding values from PolyAggInfo, limit, and date range.
    fn build(base_query: &str, poly_agg_info: &PolyAggInfo, limit: &str, date_range: &[String]) -> Vec<String> {
        date_range
            .iter()
            .map(|date| {
                base_query
                    .replace("{ticker}", &poly_agg_info.ticker)
                    .replace("{start_date}", date)
                    .replace("{end_date}", date)
                    .replace("{limit}", limit)
            })
            .collect()
    }
}

/// Sends requests to the Polygon API and handles the responses.
struct RequestSender;

impl RequestSender {
    /// Sends requests to the Polygon API based on the provided queries.
    ///
    /// Returns a tuple containing the successful responses and failed requests.
    async fn send(queries: Vec<String>) -> (Vec<(String, reqwest::Response)>, Vec<(String, String)>) {
        let futures = queries.into_iter().map(|query| {
            let date = extract_date(&query);
            tokio::spawn(async move {
                match PolygonHistorySession::send_request(&query).await {
                    Ok(response) => (date, Ok(response)),
                    Err(error) => (query, Err(error.to_string())),
                }
            })
        });

        let results: Vec<_> = join_all(futures).await;
        let (successful_responses, failed_requests): (Vec<_>, Vec<_>) = results
            .into_iter()
            .map(|result| result.unwrap())
            .partition(|(_, result)| result.is_ok());

        let successful_responses: Vec<_> = successful_responses
            .into_iter()
            .map(|(date, result)| (date, result.unwrap()))
            .collect();

        let failed_requests: Vec<_> = failed_requests
            .into_iter()
            .map(|(date, result)| (date, result.unwrap_err()))
            .collect();

        (successful_responses, failed_requests)
    }

    /// Retries failed requests and updates the combined DataFrame with the successful responses.
    async fn retry_failed(
        failed_requests: &[(String, String)],
        agg_data_schema: &Arc<Schema>,
        combined_df: &mut DataFrame,
    ) -> Result<(), PolarsError> {
        let mut remaining_failed_requests = failed_requests.to_vec();
        let mut retry_count = 0;

        while !remaining_failed_requests.is_empty() && retry_count < 5 {
            let retry_queries: Vec<_> = remaining_failed_requests.iter().map(|(query, _)| query.clone()).collect();
            let (successful_retries, new_failed_requests) = RequestSender::send(retry_queries).await;

            let df_vec = ResponseProcessor::process_all(successful_retries, agg_data_schema).await?;
            DataFrameBuilder::combine(combined_df, df_vec)?;

            remaining_failed_requests = new_failed_requests;
            retry_count += 1;

            if !remaining_failed_requests.is_empty() {
                let backoff_duration = Duration::from_secs(2u64.pow(retry_count));
                sleep(backoff_duration).await;
            }
        }

        if !remaining_failed_requests.is_empty() {
            eprintln!("Failed requests after retries: {:?}", remaining_failed_requests);
            return Err(PolarsError::ComputeError("Failed to process all queries".into()));
        }

        Ok(())
    }
}

/// Extracts the date from a query string.
fn extract_date(query: &str) -> String {
    query
        .split('/')
        .nth(10)
        .and_then(|segment| segment.split('?').next())
        .map(|date| date.to_string())
        .unwrap_or_else(|| query.to_string())
}

/// Processes the responses from the Polygon API.
struct ResponseProcessor;

impl ResponseProcessor {
    /// Processes all responses and returns a vector of DataFrames.
    async fn process_all(
        responses: Vec<(String, reqwest::Response)>,
        agg_data_schema: &Arc<Schema>,
    ) -> Result<Vec<DataFrame>, PolarsError> {
        let mut df_vec = Vec::new();

        for (date, response) in responses {
            if let Some(df) = ResponseProcessor::process_single((date, Ok(response)), agg_data_schema).await? {
                df_vec.push(df);
            }
        }

        Ok(df_vec)
    }

    /// Processes a single response and returns an optional DataFrame.
    async fn process_single(
        result: (String, reqwest::Result<reqwest::Response>),
        agg_data_schema: &Arc<Schema>,
    ) -> Result<Option<DataFrame>, PolarsError> {
        let (date, response) = result;
        match response {
            Ok(res) => {
                let body = res.text().await.unwrap();
                if let Ok(json) = serde_json::from_str::<Value>(&body) {
                    DataFrameBuilder::from_json(&json, &date, agg_data_schema)
                } else {
                    eprintln!("Error parsing JSON: {}", body);
                    Ok(None)
                }
            }
            Err(error) => {
                eprintln!("Error: {}", error);
                Ok(None)
            }
        }
    }
}

/// Builds DataFrames from the processed responses.
struct DataFrameBuilder;

impl DataFrameBuilder {
    /// Creates the schema for the aggregate data.
    const MAPPING: &'static [(&'static str, &'static str)] = &[
        ("c", "close"),
        ("o", "open"),
        ("vw", "vwap"),
        ("h", "high"),
        ("a", "average"),
        ("l", "low"),
        ("t", "time"),
        ("n", "transactions"),
        ("v", "volume"),
    ];

    fn create_schema() -> Arc<Schema> {
        Arc::new(Schema::from_iter(vec![
            Field::new("v", DataType::Int64),
            Field::new("vw", DataType::Float64),
            Field::new("o", DataType::Float64),
            Field::new("c", DataType::Float64),
            Field::new("h", DataType::Float64),
            Field::new("l", DataType::Float64),
            Field::new("t", DataType::Int64),
            Field::new("n", DataType::Int64),
        ]))
    }

    /// Creates an empty DataFrame.
    fn create_empty() -> DataFrame {
        DataFrame::default()
    }

    /// Creates a DataFrame from JSON data.
    fn from_json(json: &Value, date: &str, agg_data_schema: &Arc<Schema>) -> Result<Option<DataFrame>, PolarsError> {
        if let Some(results_count) = json["resultsCount"].as_u64() {
            if results_count > 0 {
                if let Some(results) = json["results"].as_array() {
                    if let Ok(json_string) = to_string(results) {
                        let mut df = JsonReader::new(Cursor::new(json_string))
                            .with_schema(SchemaRef::from((*agg_data_schema).clone()))
                            .finish()?;
                        DataFrameBuilder::add_date_column(&mut df, date);
                        DataFrameBuilder::rename_columns(&mut df); 
                        return Ok(Some(df));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Renames the columns of the DataFrame based on the provided mapping.
    fn rename_columns(df: &mut DataFrame) {
        for (old_name, new_name) in Self::MAPPING {
            if let Ok(_) = df.rename(old_name, new_name) {
                // Column renamed successfully
            }
        }
    }
    /// Adds a date column to the DataFrame.
    fn add_date_column(df: &mut DataFrame, date: &str) {
        if !df.is_empty() {
            let date_column = Series::new("mkt_date", vec![date.to_string(); df.height()]);
            df.with_column(date_column).unwrap();
        }
    }

    /// Adds a ticker column to the DataFrame.
    fn add_ticker_column(df: &mut DataFrame, ticker: &str) {
        if !df.is_empty() {
            let ticker_column = Series::new("ticker", vec![ticker.to_string(); df.height()]);
            df.with_column(ticker_column).unwrap();
        }
    }

    /// Combines a vector of DataFrames into the combined DataFrame.
    fn combine(combined_df: &mut DataFrame, df_vec: Vec<DataFrame>) -> Result<(), PolarsError> {
        if let Some(df) = df_vec.into_iter().reduce(|mut acc, df| {
            acc.vstack_mut(&df).unwrap();
            acc
        }) {
            combined_df.vstack_mut(&df)?;
        }
        Ok(())
    }

    /// Finalizes the combined DataFrame by adding a ticker column and sorting by date.
    fn finalize(combined_df: &mut DataFrame, ticker: &str) -> Result<(), PolarsError> {
        DataFrameBuilder::add_ticker_column(combined_df, ticker);
        combined_df.sort_in_place(&["mkt_date"], SortMultipleOptions::default())?;
        Ok(())
    }
}