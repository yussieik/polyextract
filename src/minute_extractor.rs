// src/minute_extractor.rs

use super::data_extractor::AggDataExtractor;
use super::poly_agg_info::PolyAggInfo;

pub struct MinuteExtractor {
    pub extractor: AggDataExtractor,
}

impl MinuteExtractor {
    pub fn new(poly_agg_info: PolyAggInfo) -> Self {
        let base_query = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}".to_string();
        let limit = "5000".to_string();
        let data_extractor = AggDataExtractor {
            poly_agg_info,
            base_query,
            limit,
        };
        MinuteExtractor { extractor: data_extractor }
    }
}