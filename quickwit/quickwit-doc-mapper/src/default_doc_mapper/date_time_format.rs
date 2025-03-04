// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::fmt::Display;
use std::str::FromStr;

use ouroboros::self_referencing;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use time::error::Format;
use time::format_description::FormatItem;
use time::{OffsetDateTime, PrimitiveDateTime};
use time_fmt::parse::time_format_item::parse_to_format_item;

/// A date time parser that holds the format specification `Vec<FormatItem>`.
#[self_referencing]
pub struct StrptimeParser {
    strptime_format: String,
    with_timezone: bool,
    #[borrows(strptime_format)]
    #[covariant]
    items: Vec<FormatItem<'this>>,
}

impl FromStr for StrptimeParser {
    type Err = String;

    fn from_str(strptime_format_str: &str) -> Result<Self, Self::Err> {
        StrptimeParser::try_new(
            strptime_format_str.to_string(),
            strptime_format_str.to_lowercase().contains("%z"),
            |strptime_format: &String| {
                parse_to_format_item(strptime_format).map_err(|err| {
                    format!("Invalid format specification `{strptime_format}`. Error: {err}.")
                })
            },
        )
    }
}

impl StrptimeParser {
    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        if *self.borrow_with_timezone() {
            OffsetDateTime::parse(date_time_str, self.borrow_items()).map_err(|err| err.to_string())
        } else {
            PrimitiveDateTime::parse(date_time_str, self.borrow_items())
                .map(|date_time| date_time.assume_utc())
                .map_err(|err| err.to_string())
        }
    }

    pub fn format_date_time(&self, date_time: &OffsetDateTime) -> Result<String, Format> {
        date_time.format(self.borrow_items())
    }
}

impl Clone for StrptimeParser {
    fn clone(&self) -> Self {
        // `self.format` is already known to be a valid format.
        Self::from_str(self.borrow_strptime_format().as_str()).unwrap()
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_strptime_format() == other.borrow_strptime_format()
    }
}

impl Eq for StrptimeParser {}

impl std::fmt::Debug for StrptimeParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StrptimeParser")
            .field("format", &self.borrow_strptime_format())
            .finish()
    }
}

impl std::hash::Hash for StrptimeParser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.borrow_strptime_format().hash(state);
    }
}

// `Strftime` format special characters.
// These characters are taken from the parsing crate we use for compatibility.
const STRFTIME_FORMAT_MARKERS: [&str; 36] = [
    "%a", "%A", "%b", "%B", "%c", "%C", "%d", "%D", "%e", "%f", "%F", "%h", "%H", "%I", "%j", "%k",
    "%l", "%m", "%M", "%n", "%p", "%P", "%r", "%R", "%S", "%t", "%T", "%U", "%w", "%W", "%x", "%X",
    "%y", "%Y", "%z", "%Z",
];

// Checks if a format contains `strftime` special characters.
fn is_strftime_formatting(format_str: &str) -> bool {
    for marker in STRFTIME_FORMAT_MARKERS.iter() {
        if format_str.contains(marker) {
            return true;
        }
    }
    false
}

/// Specifies the datetime and unix timestamp formats to use when parsing date strings.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeInputFormat {
    Iso8601,
    Rfc2822,
    Rfc3339,
    Strptime(StrptimeParser),
    Timestamp,
}

impl Default for DateTimeInputFormat {
    fn default() -> Self {
        DateTimeInputFormat::Rfc3339
    }
}

impl DateTimeInputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeInputFormat::Iso8601 => "iso8601",
            DateTimeInputFormat::Rfc2822 => "rfc2822",
            DateTimeInputFormat::Rfc3339 => "rfc3339",
            DateTimeInputFormat::Strptime(parser) => parser.borrow_strptime_format(),
            DateTimeInputFormat::Timestamp => "unix_timestamp",
        }
    }
}

impl Display for DateTimeInputFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DateTimeInputFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeInputFormat::Iso8601,
            "rfc2822" => DateTimeInputFormat::Rfc2822,
            "rfc3339" => DateTimeInputFormat::Rfc3339,
            "unix_timestamp" => DateTimeInputFormat::Timestamp,
            _ => {
                if !is_strftime_formatting(date_time_format_str) {
                    return Err(format!(
                        "Unknown input format: `{date_time_format_str}`. A custom date time \
                         format must contain at least one `strftime` special characters."
                    ));
                }
                DateTimeInputFormat::Strptime(StrptimeParser::from_str(date_time_format_str)?)
            }
        };
        Ok(date_time_format)
    }
}

impl Serialize for DateTimeInputFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DateTimeInputFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_format_str: String = Deserialize::deserialize(deserializer)?;
        let date_time_format = date_time_format_str.parse().map_err(D::Error::custom)?;
        Ok(date_time_format)
    }
}

/// Specifies the datetime format to use when displaying datetime values.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeOutputFormat {
    Iso8601,
    Rfc2822,
    Rfc3339,
    Strptime(StrptimeParser),
    TimestampSecs,
    TimestampMillis,
    TimestampMicros,
}

impl Default for DateTimeOutputFormat {
    fn default() -> Self {
        DateTimeOutputFormat::Rfc3339
    }
}

impl DateTimeOutputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeOutputFormat::Iso8601 => "iso8601",
            DateTimeOutputFormat::Rfc2822 => "rfc2822",
            DateTimeOutputFormat::Rfc3339 => "rfc3339",
            DateTimeOutputFormat::Strptime(parser) => parser.borrow_strptime_format(),
            DateTimeOutputFormat::TimestampSecs => "unix_timestamp_secs",
            DateTimeOutputFormat::TimestampMillis => "unix_timestamp_millis",
            DateTimeOutputFormat::TimestampMicros => "unix_timestamp_micros",
        }
    }
}

impl Display for DateTimeOutputFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DateTimeOutputFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeOutputFormat::Iso8601,
            "rfc2822" => DateTimeOutputFormat::Rfc2822,
            "rfc3339" => DateTimeOutputFormat::Rfc3339,
            "unix_timestamp_secs" => DateTimeOutputFormat::TimestampSecs,
            "unix_timestamp_millis" => DateTimeOutputFormat::TimestampMillis,
            "unix_timestamp_micros" => DateTimeOutputFormat::TimestampMicros,
            _ => {
                if !is_strftime_formatting(date_time_format_str) {
                    return Err(format!(
                        "Unknown output format: `{date_time_format_str}`. A custom date time \
                         format must contain at least one `strftime` special characters."
                    ));
                }
                DateTimeOutputFormat::Strptime(StrptimeParser::from_str(date_time_format_str)?)
            }
        };
        Ok(date_time_format)
    }
}

impl Serialize for DateTimeOutputFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DateTimeOutputFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_format_str: String = Deserialize::deserialize(deserializer)?;
        let date_time_format = date_time_format_str.parse().map_err(D::Error::custom)?;
        Ok(date_time_format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_time_input_format_ser() {
        let date_time_formats_json = serde_json::to_value(&[
            DateTimeInputFormat::Iso8601,
            DateTimeInputFormat::Rfc2822,
            DateTimeInputFormat::Rfc3339,
            DateTimeInputFormat::Timestamp,
        ])
        .unwrap();

        let expected_date_time_formats =
            serde_json::json!(["iso8601", "rfc2822", "rfc3339", "unix_timestamp",]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_input_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_timestamp"
            ]
            "#;
        let date_time_formats: Vec<DateTimeInputFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeInputFormat::Iso8601,
            DateTimeInputFormat::Rfc2822,
            DateTimeInputFormat::Rfc3339,
            DateTimeInputFormat::Timestamp,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }

    #[test]
    fn test_date_time_output_format_ser() {
        let date_time_formats_json = serde_json::to_value(&[
            DateTimeOutputFormat::Iso8601,
            DateTimeOutputFormat::Rfc2822,
            DateTimeOutputFormat::Rfc3339,
            DateTimeOutputFormat::TimestampSecs,
            DateTimeOutputFormat::TimestampMillis,
            DateTimeOutputFormat::TimestampMicros,
        ])
        .unwrap();

        let expected_date_time_formats = serde_json::json!([
            "iso8601",
            "rfc2822",
            "rfc3339",
            "unix_timestamp_secs",
            "unix_timestamp_millis",
            "unix_timestamp_micros",
        ]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_output_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_timestamp_secs",
                "unix_timestamp_millis",
                "unix_timestamp_micros"
            ]
            "#;
        let date_time_formats: Vec<DateTimeOutputFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeOutputFormat::Iso8601,
            DateTimeOutputFormat::Rfc2822,
            DateTimeOutputFormat::Rfc3339,
            DateTimeOutputFormat::TimestampSecs,
            DateTimeOutputFormat::TimestampMillis,
            DateTimeOutputFormat::TimestampMicros,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }

    #[test]
    fn test_fail_date_time_input_format_from_str_with_unknown_format() {
        let formats = vec![
            "test%",
            "test-%v",
            "test-%q",
            "unix_timestamp_secs",
            "unix_timestamp_seconds",
        ];
        for format in formats {
            let error_str = DateTimeInputFormat::from_str(format)
                .unwrap_err()
                .to_string();
            assert!(error_str.contains(&format!("Unknown input format: `{format}`.")));
        }
    }

    #[test]
    fn test_fail_date_time_output_format_from_str_with_unknown_format() {
        let formats = vec!["test%", "test-%v", "test-%q", "unix_timestamp_seconds"];
        for format in formats {
            let error_str = DateTimeOutputFormat::from_str(format)
                .unwrap_err()
                .to_string();
            assert!(error_str.contains(&format!("Unknown output format: `{format}`.")));
        }
    }
}
