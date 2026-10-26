//! Common WebSocket domain types, scaling constants, and timestamp parsers.

use crate::polymarket::dtos::{MarketResolved, NewMarket};
use chrono::{DateTime, Utc};

/// Multiplier applied to string decimal prices and quantities to produce integer fixed-point values (1 unit = 1_000_000).
pub const DEFAULT_SCALE: i64 = 1_000_000;

/// Role of a WebSocket connection within the same-pool 2-copy redundancy topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsRole {
    /// Active writer: processes incoming deltas and snapshots, appending encoded messages to the disk-bound batch buffer.
    Primary,
    /// Hot standby: subscribed to the same symbols as its paired Primary and maintains resting orderbook depth in RAM, ready for instant promotion.
    Replica,
}

/// Market lifecycle events emitted by the designated Master anchor socket when new markets appear or existing markets resolve.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscoveryEvent {
    NewMarket(Box<NewMarket>),
    MarketResolved(Box<MarketResolved>),
}

/// Counts the number of decimal digits in a power-of-10 scale factor by repeatedly dividing by 10 until the value reaches 1.
#[inline]
pub fn scale_digits(mut scale: i64) -> usize {
    let mut digits = 0;
    while scale > 1 {
        scale /= 10;
        digits += 1;
    }
    digits
}

/// Converts a string decimal (e.g. `"0.48"`, `"30"`, `"48714.92"`) into a fixed-point integer
/// by multiplying with the given scale factor using integer arithmetic, avoiding floating-point rounding errors.
pub fn parse_fixed_point_scale(val_str: &str, scale: i64) -> anyhow::Result<i64> {
    let s = val_str.trim();
    if s.is_empty() {
        return Ok(0);
    }

    // Fast path: standard decimal notation without scientific exponent
    if !s.contains('e') && !s.contains('E') {
        let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix('+') {
            (false, rest)
        } else {
            (false, s)
        };

        let mut parts = s.splitn(2, '.');
        let int_str = parts.next().unwrap_or("0");
        let frac_str = parts.next().unwrap_or("");

        let int_val: i64 = if int_str.is_empty() {
            0
        } else {
            int_str.parse()?
        };

        let frac_val: i64 = if !frac_str.is_empty() {
            let scale_digits = scale_digits(scale);
            let len = frac_str.len();
            if len <= scale_digits {
                let frac_parsed: i64 = frac_str.parse()?;
                let multiplier = 10_i64.pow((scale_digits - len) as u32);
                frac_parsed * multiplier
            } else {
                let prefix: i64 = frac_str[..scale_digits].parse()?;
                let round_up = if frac_str.as_bytes()[scale_digits] >= b'5' {
                    1
                } else {
                    0
                };
                prefix + round_up
            }
        } else {
            0
        };

        let total = int_val * scale + frac_val;
        return Ok(if neg { -total } else { total });
    }

    // Fallback: scientific notation via f64 round
    let val: f64 = s.parse()?;
    Ok((val * scale as f64).round() as i64)
}

/// Converts a Unix millisecond timestamp string or an RFC 3339 datetime string into a `DateTime<Utc>`, falling back to the current wall-clock time when parsing fails.
pub fn parse_exchange_timestamp(ts_str: &str) -> DateTime<Utc> {
    let s = ts_str.trim();
    if let Ok(ms) = s.parse::<i64>() {
        DateTime::from_timestamp_millis(ms).unwrap_or_else(crate::util::local_now)
    } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        dt.with_timezone(&Utc)
    } else {
        crate::util::local_now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fixed_point_scale() {
        assert_eq!(parse_fixed_point_scale("0.48", 1_000_000).unwrap(), 480_000);
        assert_eq!(parse_fixed_point_scale("1", 1_000_000).unwrap(), 1_000_000);
        assert_eq!(parse_fixed_point_scale("0.000001", 1_000_000).unwrap(), 1);
        assert_eq!(
            parse_fixed_point_scale("100.5", 1_000_000).unwrap(),
            100_500_000
        );
        assert_eq!(
            parse_fixed_point_scale("-0.25", 1_000_000).unwrap(),
            -250_000
        );
        assert_eq!(
            parse_fixed_point_scale("0.001", DEFAULT_SCALE).unwrap(),
            1_000
        );
    }
}
