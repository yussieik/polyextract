// src/extractors/poly_agg_info.rs

use chrono::NaiveDate;

#[derive(Clone)]
pub struct PolyAggInfo {
    pub ticker: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub resolution: String,
    pub multiplier: u32,
}

impl PolyAggInfo {
    pub fn create_poly_agg_infos(
        tickers: Vec<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        resolution: String,
        multiplier: u32,
    ) -> Vec<Self> {
        tickers
            .into_iter()
            .map(|ticker| PolyAggInfo {
                ticker,
                start_date,
                end_date,
                resolution: resolution.clone(),
                multiplier,
            })
            .collect()
    }
}