// src/ticker_manager.rs

use crate::minute_extractor::MinuteExtractor;
use crate::poly_agg_info::PolyAggInfo;
use crate::processor::Processor;
use crate::processor::MarketTimezone;
use polars::prelude::*;
use async_trait::async_trait;

pub struct TickerManager {
    poly_agg_info: PolyAggInfo,
}

impl TickerManager {
    pub fn new(poly_agg_info: PolyAggInfo) -> Self {
        TickerManager { poly_agg_info }
    }

    pub async fn process_data(&self) -> Result<DataFrame, PolarsError> {
        // 1. Upload the data using the correct Strategy based on the resolution value
        let strategy = self.create_strategy();
        let mut df = strategy.extract_data().await?;

        // 2. Use the Processor struct to process the uploaded data
        let market_timezone = MarketTimezone::Eastern;
        let mut processor = Processor::new(&mut df, &market_timezone);
        let _ = processor.process()?;

        // 3. Store the resulting DataFrame for saving afterwards
        Ok(processor.df.clone())
    }

    fn create_strategy(&self) -> Box<dyn Strategy> {
        match self.poly_agg_info.resolution.as_str() {
            "minute" => Box::new(MinuteExtractor::new(self.poly_agg_info.clone())),
            _ => panic!("Unsupported resolution"),
        }
    }
}

#[async_trait]
trait Strategy {
    async fn extract_data(&self) -> Result<DataFrame, PolarsError>;
}

#[async_trait]
impl Strategy for MinuteExtractor {
    async fn extract_data(&self) -> Result<DataFrame, PolarsError> {
        self.extractor.extract().await
    }
}