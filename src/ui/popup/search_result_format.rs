use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};

pub(crate) fn format_search_result_text(value: &str) -> String {
    format_search_result_text_at(value, Local::now().naive_local())
}

fn format_search_result_text_at(value: &str, now: NaiveDateTime) -> String {
    let trimmed = value.trim();

    if let Some(dt) = parse_datetime_text(trimmed) {
        return format!("{} ({})", format_relative_datetime(dt, now), value);
    }

    if let Some(date) = parse_date_text(trimmed) {
        return format!("{} ({})", format_relative_date(date, now.date()), value);
    }

    value.to_string()
}

fn parse_date_text(text: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()
}

fn parse_datetime_text(text: &str) -> Option<NaiveDateTime> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt.naive_local());
    }

    for pattern in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, pattern) {
            return Some(dt);
        }
    }

    None
}

fn format_relative_date(date: NaiveDate, today: NaiveDate) -> String {
    let delta_days = date.signed_duration_since(today).num_days();
    match delta_days {
        0 => "today".to_string(),
        -1 => "yesterday".to_string(),
        1 => "tomorrow".to_string(),
        days if days < 0 => format!("{} ago", compact_days(-days)),
        days => format!("in {}", compact_days(days)),
    }
}

fn format_relative_datetime(dt: NaiveDateTime, now: NaiveDateTime) -> String {
    let delta_seconds = dt.signed_duration_since(now).num_seconds();
    let abs_seconds = delta_seconds.unsigned_abs();

    let label = if abs_seconds < 60 {
        "just now".to_string()
    } else if abs_seconds < 60 * 60 {
        format!("{}m", abs_seconds / 60)
    } else if abs_seconds < 60 * 60 * 24 {
        format!("{}h", abs_seconds / (60 * 60))
    } else if abs_seconds < 60 * 60 * 24 * 30 {
        format!("{}d", abs_seconds / (60 * 60 * 24))
    } else if abs_seconds < 60 * 60 * 24 * 365 {
        format!("{}mo", abs_seconds / (60 * 60 * 24 * 30))
    } else {
        format!("{}y", abs_seconds / (60 * 60 * 24 * 365))
    };

    match delta_seconds.cmp(&0) {
        std::cmp::Ordering::Less => {
            if label == "just now" {
                label
            } else {
                format!("{} ago", label)
            }
        }
        std::cmp::Ordering::Equal => label,
        std::cmp::Ordering::Greater => {
            if label == "just now" {
                label
            } else {
                format!("in {}", label)
            }
        }
    }
}

fn compact_days(days: i64) -> String {
    if days < 30 {
        format!("{days}d")
    } else if days < 365 {
        format!("{}mo", days / 30)
    } else {
        format!("{}y", days / 365)
    }
}

#[cfg(test)]
mod tests {
    use super::format_search_result_text_at;
    use chrono::NaiveDate;

    #[test]
    fn formats_date_values_relatively() {
        let now = NaiveDate::from_ymd_opt(2026, 4, 26)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        assert_eq!(
            format_search_result_text_at("2026-04-25", now),
            "yesterday (2026-04-25)"
        );
        assert_eq!(
            format_search_result_text_at("2026-05-03", now),
            "in 7d (2026-05-03)"
        );
    }

    #[test]
    fn formats_datetime_values_relatively() {
        let now = NaiveDate::from_ymd_opt(2026, 4, 26)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        assert_eq!(
            format_search_result_text_at("2026-04-26T10:30:00", now),
            "1h ago (2026-04-26T10:30:00)"
        );
        assert_eq!(
            format_search_result_text_at("2026-04-26T15:00:00", now),
            "in 3h (2026-04-26T15:00:00)"
        );
    }

    #[test]
    fn leaves_non_dates_untouched() {
        let now = NaiveDate::from_ymd_opt(2026, 4, 26)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        assert_eq!(format_search_result_text_at("Alice", now), "Alice");
    }
}
