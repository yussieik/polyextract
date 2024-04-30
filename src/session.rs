// src/session.rs 


use lazy_static::lazy_static;
use reqwest::{Client, Response};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::config::{MAX_BURST_REQUESTS, POLYGON_API_KEY, REQUESTS_PER_SECOND};


lazy_static! {
    static ref SINGLETON_SESSION: Arc<Mutex<PolygonHistorySession >> = Arc::new(Mutex::new(PolygonHistorySession::new()));
}

pub struct PolygonHistorySession {
    client: Client,
    rate_limiter: Arc<Mutex<PolygonRateLimiter>>,
}

impl PolygonHistorySession {
    fn new() -> Self {
        PolygonHistorySession {
            client: Client::new(),
            rate_limiter: Arc::new(Mutex::new(PolygonRateLimiter::new(REQUESTS_PER_SECOND))),
        }
    }

    pub async fn send_request(url: &str) -> Result<Response, reqwest::Error> {
        let session = Arc::clone(&SINGLETON_SESSION);
        let url_with_api_key = format!("{}&apiKey={}", url, POLYGON_API_KEY);

        let (client, rate_limiter) = {
            let session = session.lock().await;
            (session.client.clone(), Arc::clone(&session.rate_limiter))
        };

        rate_limiter.lock().await.acquire().await;

        client.get(&url_with_api_key).send().await
    }
}

struct PolygonRateLimiter {
    tokens: u32,
    last_refill_time: Instant,
    refill_interval: Duration,
}

impl PolygonRateLimiter {
    fn new(requests_per_second: u32) -> Self {
        PolygonRateLimiter {
            tokens: MAX_BURST_REQUESTS,
            last_refill_time: Instant::now(),
            refill_interval: Duration::from_secs(1) / requests_per_second,
        }
    }

    async fn acquire(&mut self) {
        while self.tokens == 0 {
            let now = Instant::now();
            let elapsed = now - self.last_refill_time;

            if elapsed >= self.refill_interval {
                let refill_count = (elapsed.as_secs_f32() / self.refill_interval.as_secs_f32()) as u32;
                self.tokens = std::cmp::min(self.tokens + refill_count, MAX_BURST_REQUESTS);
                self.last_refill_time = now;
            } else {
                sleep(self.refill_interval - elapsed).await;
            }
        }

        self.tokens -= 1;
    }
}