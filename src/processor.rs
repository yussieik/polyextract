extern crate chrono;
extern crate chrono_tz;
extern crate polars;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::Tz;
use polars::prelude::*;
use rayon::prelude::*;

pub struct Processor<'a, 'b> {
    pub df: &'a mut DataFrame,
    market: &'b MarketTimezone,
}

impl<'a, 'b> Processor<'a, 'b> {
    pub fn new(df: &'a mut DataFrame, market: &'b MarketTimezone) -> Self {
        Processor { df, market }
    }

    pub fn process(&mut self) -> Result<(usize, usize), PolarsError> {
        let mut total_p1_outliers = 0;
        let mut total_p2_outliers = 0;

        let processed_df = self.df.group_by(["mkt_date"])?.apply(|group_df| {
            let mkt_date_column = group_df.column("mkt_date")?.str()?;
            let mkt_date = mkt_date_column.get(0).ok_or(PolarsError::NoData("mkt_date is empty".into()))?;

            let time_filter = MarketHoursFilter::new(&group_df, self.market, mkt_date);
            let mut filtered_df = time_filter.filter()?;

            let mut outlier_detector = MADOutlierDetector::new(&mut filtered_df);
            let (p1_outliers, p2_outliers) = outlier_detector.detect_normalize()?;

            total_p1_outliers += p1_outliers;
            total_p2_outliers += p2_outliers;

            Ok(filtered_df)
        })?;

        *self.df = processed_df;

        Ok((total_p1_outliers, total_p2_outliers))
    }
}

pub struct MADOutlierDetector<'a> {
    df: &'a mut DataFrame,
    p1_p2_df: DataFrame,
}

impl<'a> MADOutlierDetector<'a> {
    pub fn new(df: &'a mut DataFrame) -> Self {
        let p1_p2_df = DataFrame::new(vec![
            Series::new("p1", &vec![None::<f64>; df.height()]),
            Series::new("p2", &vec![None::<f64>; df.height()]),
            Series::new("p1_outlier", &vec![None::<bool>; df.height()]),
            Series::new("p2_outlier", &vec![None::<bool>; df.height()]),
        ]).unwrap();

        MADOutlierDetector { df, p1_p2_df }
    }

    pub fn calculate_p1_p2(&mut self) -> Result<(), PolarsError> {
        let h = self.df.column("high")?.f64()?;
        let c = self.df.column("close")?.f64()?;
        let o = self.df.column("open")?.f64()?;
        let l = self.df.column("low")?.f64()?;

        let mut p1 = Vec::with_capacity(h.len());
        let mut p2 = Vec::with_capacity(h.len());

        for i in 0..h.len() {
            let p1_value = if c.get(i) >= o.get(i) {
                h.get(i).zip(c.get(i)).map(|(high, close)| high - close)
            } else {
                h.get(i).zip(o.get(i)).map(|(high, open)| high - open)
            };

            let p2_value = if c.get(i) >= o.get(i) {
                o.get(i).zip(l.get(i)).map(|(open, low)| open - low)
            } else {
                c.get(i).zip(l.get(i)).map(|(close, low)| close - low)
            };

            p1.push(p1_value);
            p2.push(p2_value);
        }

        self.p1_p2_df.with_column(Series::new("p1", p1))?;
        self.p1_p2_df.with_column(Series::new("p2", p2))?;

        self.detect_outliers("p1", 15.0)?;
        self.detect_outliers("p2", 15.0)?;

        Ok(())
    }

    fn detect_outliers(&mut self, column_name: &str, threshold: f64) -> Result<usize, PolarsError> {
        let column = self.p1_p2_df.column(column_name)?.f64()?;
        let median = column.median().unwrap_or_default();

        // Compute absolute deviation from median and then calculate the median of those deviations
        let deviations = column.apply(|value| value.map(|v| (v - median).abs()));
        let mad = deviations.median().unwrap_or_default();

        // Apply outlier detection based on the computed MAD
        let outliers = deviations.apply(|deviation| {
            deviation.map(|dev| {
                if dev > (mad * threshold) {
                    1.0
                } else {
                    0.0
                }
            })
        });

        let outliers_sum = outliers.sum().unwrap_or_default() as usize;
        self.p1_p2_df.with_column(Series::new(&format!("{}_outlier", column_name), outliers))?;

        Ok(outliers_sum)
    }


    pub fn normalize_outliers(&mut self, p_type: &str) -> Result<(), PolarsError> {
        let outlier_col_name = format!("{}_outlier", p_type);
        let outlier_series = self.p1_p2_df.column(&outlier_col_name)?.f64()?;

        let open_series = self.df.column("open")?.f64()?;
        let close_series = self.df.column("close")?.f64()?;
        let mut high_values = self.df.column("high")?.f64()?.into_no_null_iter().collect::<Vec<_>>();
        let mut low_values = self.df.column("low")?.f64()?.into_no_null_iter().collect::<Vec<_>>();

        for (i, opt_is_outlier) in outlier_series.into_iter().enumerate() {
            if let Some(is_outlier) = opt_is_outlier {
                if is_outlier == 1.0 {
                    let (open, close) = (open_series.get(i), close_series.get(i));
                    match p_type {
                        "p1" => adjust_value(open, close, &mut high_values, i, |o, c| o > c),
                        "p2" => adjust_value(open, close, &mut low_values, i, |o, c| o < c),
                        _ => return Err(PolarsError::ComputeError("Invalid p_type provided".into())),
                    }
                }
            }
        }

        // Replace the old columns with new modified data
        self.df.replace("high", Series::new("high", &high_values))?;
        self.df.replace("low", Series::new("low", &low_values))?;

        Ok(())
    }

    pub fn detect_normalize(&mut self) -> Result<(usize, usize), PolarsError> {
        // Calculate p1 and p2
        self.calculate_p1_p2()?;

        // Detect outliers
        let threshold = 15.0;
        let p1_outliers = self.detect_outliers("p1", threshold)?;
        let p2_outliers = self.detect_outliers("p2", threshold)?;

        // Normalize outliers
        self.normalize_outliers("p1")?;
        self.normalize_outliers("p2")?;

        Ok((p1_outliers, p2_outliers))
    }
}

fn adjust_value<F>(open: Option<f64>, close: Option<f64>, values: &mut Vec<f64>, index: usize, comparator: F)
    where F: Fn(f64, f64) -> bool {
    if let (Some(o), Some(c)) = (open, close) {
        values[index] = if comparator(o, c) { o } else { c };
    }
}



pub enum MarketTimezone {
    Eastern,
    // Additional market timezones can be added here
}

impl MarketTimezone {
    // Returns the static working hours for the market
    pub fn working_hours(&self) -> (NaiveTime, NaiveTime) {
        match self {
            MarketTimezone::Eastern => {
                (NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                 NaiveTime::from_hms_opt(16, 0, 0).unwrap())
            }
            // Add other timezones with their working hours as needed
        }
    }

    // Returns the timezone corresponding to the market
    pub fn timezone(&self) -> Tz {
        match self {
            MarketTimezone::Eastern => chrono_tz::US::Eastern,
            // Add other timezones as needed
        }
    }

    // Calculates and returns the start and end DateTime for a given date
    pub fn market_hours_on_date(&self, date: &str) -> Result<(DateTime<Tz>, DateTime<Tz>), String> {
        let naive_date = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|e| e.to_string())?;
        let (start_time, end_time) = self.working_hours();
        let timezone = self.timezone();

        let start_datetime = NaiveDateTime::new(naive_date, start_time);
        let end_datetime = NaiveDateTime::new(naive_date, end_time);

        let start_tz_datetime = timezone.from_local_datetime(&start_datetime).single().ok_or_else(|| "Unable to determine unique timezone datetime for start".to_string())?;
        let end_tz_datetime = timezone.from_local_datetime(&end_datetime).single().ok_or_else(|| "Unable to determine unique timezone datetime for end".to_string())?;

        Ok((start_tz_datetime, end_tz_datetime))
    }

    // Calculates and returns the start and end millisecond timestamp integers for a given date
    pub fn market_hours_on_date_millis(&self, date: &str) -> Result<(i64, i64), String> {
        let (start_tz_datetime, end_tz_datetime) = self.market_hours_on_date(date)?;

        let start_millis = start_tz_datetime.timestamp_millis();
        let end_millis = end_tz_datetime.timestamp_millis();

        Ok((start_millis, end_millis))
    }
}

pub struct MarketHoursFilter<'a, 'b> {
    df: &'a DataFrame,
    market: &'b MarketTimezone,
    date: &'a str,
}

impl<'a, 'b> MarketHoursFilter<'a, 'b> {
    pub fn new(df: &'a DataFrame, market: &'b MarketTimezone, date: &'a str) -> Self {
        MarketHoursFilter { df, market, date }
    }

    pub fn filter(&self) -> Result<DataFrame, PolarsError> {
        let (start_ts, end_ts) = self.market.market_hours_on_date_millis(self.date)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;

        let time_column = self.df.column("time")?.i64()?;
        let mask = time_column.into_iter().map(|opt_time| {
            opt_time.map(|time| time >= start_ts && time <= end_ts).unwrap_or(false)
        }).collect::<BooleanChunked>();

        self.df.filter(&mask)
    }
}

