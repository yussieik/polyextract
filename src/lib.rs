// src/lib.rs

pub mod config;
pub mod session;
pub mod data_extractor;
pub mod minute_extractor;
pub mod poly_agg_info;

pub mod processor;
mod ticker_manager;
mod ticker_manager_pool;

pub use session::PolygonHistorySession;

pub use data_extractor::AggDataExtractor;
pub use minute_extractor::MinuteExtractor;
pub use poly_agg_info::PolyAggInfo;
pub use processor::MADOutlierDetector;
pub use processor::Processor;
pub use processor::MarketTimezone;
pub use ticker_manager::TickerManager;
pub use ticker_manager_pool::TickerManagerPool;