# Dataset numbers (measured, not guessed)

**Owner:** Riya Maurya. Fill this in from real runs and add the date you measured. These numbers go straight into the report's "why big data" section, so never estimate here.

## Historical data (OpenAQ archive)

| Measure | Value | Measured on |
|---|---|---|
| Stations (India) | | |
| Stations (Delhi-NCR) | | |
| Date range | | |
| Raw files (count) | | |
| Size on disk, compressed | | |
| Rows, raw (narrow format) | | |
| Rows after pivot (one per station-hour) | | |
| Rows after features | | |
| % missing PM2.5 | | |
| % missing weather | | |

## Live data

| Measure | Value | Measured on |
|---|---|---|
| Delhi-NCR stations reporting live | | |
| Live collection started | | |
| Live messages collected so far | | |
| Typical delay: reading hour → message in Kafka | | |

## Pandas vs Spark (Ansh)

| Job | Pandas | Spark | Machine (RAM, cores) |
|---|---|---|---|
| | | | |

## Time convention

OpenAQ `ts_utc` means the **start / end** (delete one) of the hour, for both the archive and the API. Checked by:
