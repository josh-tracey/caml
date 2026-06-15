use std::{fmt, str::FromStr, time::Duration};

use serde::{de::Error as DeError, Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    pub fn as_bytes(self) -> u64 {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err("Invalid byte size format. Value cannot be empty.".to_string());
        }

        let split_at = trimmed.find(|c: char| !c.is_ascii_digit()).ok_or_else(|| {
            "Invalid byte size format. Expected a unit suffix such as 'MB'.".to_string()
        })?;
        let (digits, unit) = trimmed.split_at(split_at);
        let value = digits.parse::<u64>().map_err(|_| {
            format!(
                "Invalid byte size format. '{}' is not a valid integer.",
                digits
            )
        })?;
        let multiplier = match unit.trim().to_ascii_uppercase().as_str() {
            "B" => 1,
            "KB" => 1_000,
            "MB" => 1_000_000,
            "GB" => 1_000_000_000,
            "KIB" => 1_024,
            "MIB" => 1_048_576,
            "GIB" => 1_073_741_824,
            _ => {
                return Err(format!(
                    "Invalid byte size format. Unsupported unit '{}'.",
                    unit.trim()
                ))
            }
        };

        value
            .checked_mul(multiplier)
            .map(Self)
            .ok_or_else(|| "Invalid byte size format. Value exceeds supported range.".to_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::from_str(&raw).map_err(D::Error::custom)
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    pub fn as_bits_per_second(self) -> u64 {
        self.0
    }
}

impl FromStr for Bitrate {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err("Invalid bitrate format. Value cannot be empty.".to_string());
        }

        let split_at = trimmed.find(|c: char| !c.is_ascii_digit()).ok_or_else(|| {
            "Invalid bitrate format. Expected a unit suffix such as 'k' or 'Mbps'.".to_string()
        })?;
        let (digits, unit) = trimmed.split_at(split_at);
        let value = digits.parse::<u64>().map_err(|_| {
            format!(
                "Invalid bitrate format. '{}' is not a valid integer.",
                digits
            )
        })?;
        let normalized_unit = unit.trim().to_ascii_lowercase();

        let multiplier = match normalized_unit.as_str() {
            "k" | "kbps" => 1_000,
            "m" | "mbps" => 1_000_000,
            "g" | "gbps" => 1_000_000_000,
            "bps" => 1,
            _ => {
                return Err(format!(
                    "Invalid bitrate format. Unsupported unit '{}'.",
                    unit.trim()
                ))
            }
        };

        value
            .checked_mul(multiplier)
            .map(Self)
            .ok_or_else(|| "Invalid bitrate format. Value exceeds supported range.".to_string())
    }
}

impl<'de> Deserialize<'de> for Bitrate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::from_str(&raw).map_err(D::Error::custom)
    }
}

impl fmt::Display for Bitrate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}bps", self.0)
    }
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("Invalid duration format. Value cannot be empty.".to_string());
    }

    if let Some(value) = trimmed.strip_suffix("ms") {
        let millis = value.parse::<u64>().map_err(|_| {
            format!(
                "Invalid duration format. '{}' is not a valid millisecond value.",
                value
            )
        })?;
        return Ok(Duration::from_millis(millis));
    }

    if let Some(value) = trimmed.strip_suffix('s') {
        let secs = value.parse::<u64>().map_err(|_| {
            format!(
                "Invalid duration format. '{}' is not a valid second value.",
                value
            )
        })?;
        return Ok(Duration::from_secs(secs));
    }

    if let Some(value) = trimmed.strip_suffix('m') {
        let mins = value.parse::<u64>().map_err(|_| {
            format!(
                "Invalid duration format. '{}' is not a valid minute value.",
                value
            )
        })?;
        return mins
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| "Invalid duration format. Value exceeds supported range.".to_string());
    }

    Err("Invalid duration format. Expected a suffix such as '10s' or '250ms'.".to_string())
}

pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_duration(&raw).map_err(D::Error::custom)
}
