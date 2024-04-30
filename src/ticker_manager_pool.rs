// src/ticker_manager_pool.rs

use crate::poly_agg_info::PolyAggInfo;
use crate::ticker_manager::TickerManager;
use futures::future::join_all;
use polars::prelude::*;

pub struct TickerManagerPool {
    ticker_managers: Vec<TickerManager>,
}

impl TickerManagerPool {
    pub fn new(poly_agg_infos: Vec<PolyAggInfo>) -> Self {
        let ticker_managers = poly_agg_infos
            .into_iter()
            .map(TickerManager::new)
            .collect();
        TickerManagerPool { ticker_managers }
    }

    pub async fn process_data_concurrently(&self) -> Result<Vec<DataFrame>, PolarsError> {
        let futures = self.ticker_managers.iter().map(|manager| manager.process_data());
        let results = join_all(futures).await;
        results.into_iter().collect()
    }
}