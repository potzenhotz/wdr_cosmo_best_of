# WDR Cosmo Playlist Analyzer

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python tool to scrape and analyze WDR Cosmo radio playlists, helping you discover the most played songs by day, week, month, or custom date ranges.

## Features

- Scrape playlist data from WDR Cosmo's website
- Store playlist data in a DuckDB database (optimized for analytics)
- Analyze most played songs by day, week, month, or custom date ranges using Polars
- Find top artists
- View database statistics
- Fast analytical queries with DuckDB's columnar storage

## Installation

1. Install dependencies using uv (or pip):

```bash
uv pip install -e .
```

Or with pip:

```bash
pip install -e .
```

## Setup

Before using the scraper, you need to identify the correct CSS selectors for the WDR Cosmo playlist page.

### Automatic Inspection (Recommended)

Run the inspection helper script to automatically analyze the page structure:

```bash
python inspect_playlist.py
```

This script will:
- ✓ Fetch the WDR Cosmo playlist page from your machine
- ✓ Analyze the HTML structure automatically
- ✓ Identify potential CSS selectors for songs, artists, titles, and timestamps
- ✓ Save the HTML to `playlist_sample.html` for manual inspection
- ✓ Show you sample data extracted from the page
- ✓ Provide recommendations for updating `scraper.py`

### Manual Inspection

Alternatively, inspect manually:

1. Visit https://www1.wdr.de/radio/cosmo/musik/playlist/index.html
2. Open browser developer tools (F12)
3. Inspect the HTML structure to find:
   - Container elements for each song
   - Elements containing artist names
   - Elements containing song titles
   - Elements containing timestamps
4. Update the CSS selectors in `scraper.py` (lines 52-75)

### Update the Scraper

After identifying the selectors, update `scraper.py` around line 52-75:

```python
# Update these selectors based on inspect_playlist.py output:
song_elements = soup.select('.your-actual-selector')  # e.g., '.playlist-item'

for element in song_elements:
    artist = element.select_one('.artist-selector')
    title = element.select_one('.title-selector')
    timestamp = element.select_one('.time-selector, time')
    # ...
```

**Important**: The scraper includes placeholder selectors that must be updated based on the actual HTML structure.

## Usage

### Scrape Playlist Data

Scrape today's playlist:
```bash
python main.py scrape
```

Scrape a specific date:
```bash
python main.py scrape --date 2024-01-15
```

Scrape a date range:
```bash
python main.py scrape --start-date 2024-01-01 --end-date 2024-01-31
```

Scrape the last N days:
```bash
python main.py scrape --days 7
```

### Analyze Data

Top songs for a specific day:
```bash
python main.py top-day 2024-01-15
```

Top songs for a week (starting from specified date):
```bash
python main.py top-week 2024-01-15
```

Top songs for a month:
```bash
python main.py top-month 2024 1
```

Top songs for a custom date range:
```bash
python main.py top-range 2024-01-01 2024-01-31
```

Top songs of all time:
```bash
python main.py top-songs
```

Top songs with date filter:
```bash
python main.py top-songs --start-date 2024-01-01 --end-date 2024-01-31
```

Top artists (all time):
```bash
python main.py top-artists
```

Top artists for a date range:
```bash
python main.py top-artists --start-date 2024-01-01 --end-date 2024-01-31
```

Show database statistics:
```bash
python main.py stats
```

### Enrich with Genre Information

Add genre information from MusicBrainz to your songs:

```bash
# Enrich all songs without genre
python main.py enrich-genres

# Test with only 10 songs
python main.py enrich-genres --limit 10

# Skip confirmation prompt
python main.py enrich-genres -y

# Show verbose output (including songs not found)
python main.py enrich-genres -v
```

**Note:** MusicBrainz has a rate limit of 1 request/second, so enriching many songs takes time. The enricher respects this limit automatically.

### Options

All analysis commands support `--limit` to control the number of results:
```bash
python main.py top-day 2024-01-15 --limit 20
```

Use a custom database file:
```bash
python main.py --database my_playlist.duckdb scrape
python main.py --database my_playlist.duckdb top-day 2024-01-15
```

## Data Protection and Backups

The application includes automatic backup and data integrity verification to protect against data loss:

- **Automatic Backups**: Before any operation that modifies the database (scraping, genre enrichment), a timestamped backup is created in the `backups/` directory
- **Data Integrity Verification**: After each operation, the system verifies that no data was lost
- **Backup Location**: `backups/cosmo_playlist_backup_YYYYMMDD_HHMMSS.duckdb`

### Manual Restore

If data loss is detected or you need to restore from a backup:

```bash
# List available backups
ls -lh backups/

# Restore from a specific backup
cp backups/cosmo_playlist_backup_20260113_214057.duckdb cosmo_playlist.duckdb
```

## Project Structure

- `main.py` - CLI interface and command handlers
- `scraper.py` - Web scraping logic for WDR Cosmo playlist
- `database.py` - DuckDB database management with backup/verification
- `analyzer.py` - Data analysis using DuckDB SQL and Polars integration
- `genre_enricher.py` - MusicBrainz genre enrichment
- `cosmo_playlist.duckdb` - DuckDB database (created automatically)
- `backups/` - Automatic database backups (created before each data modification)

## Database Schema

The `songs` table contains:
- `id` - Unique identifier (auto-generated)
- `artist` - Artist name
- `title` - Song title
- `time` - Time of day when played (e.g., "17:15")
- `date` - Date when played (e.g., "2026-01-13")
- `datetime` - Full timestamp (e.g., "2026-01-13T17:15:00")
- `musicbrainz_genre` - Genre tags from MusicBrainz (e.g., "electronic, dance, house")
- `created_at` - When the record was inserted

**Note:** The `musicbrainz_genre` column is named to indicate the source and that it may not be 100% accurate. It's populated by running the `enrich-genres` command.

## Why DuckDB + Polars?

This project uses DuckDB and Polars for optimal performance:

- **DuckDB**: An analytical database optimized for OLAP workloads (aggregations, groupby operations)
  - Columnar storage for faster analytical queries
  - Native support for date/time functions
  - Excellent performance for COUNT, GROUP BY, and window functions

- **Polars**: A fast DataFrame library written in Rust
  - Direct integration with DuckDB via `.pl()` method
  - Memory-efficient operations
  - Better performance than pandas for large datasets

The combination allows for efficient SQL-based analysis with seamless conversion to Polars DataFrames for further processing.

## Next Steps

1. **Run the inspector**: Execute `python inspect_playlist.py` to analyze the page structure
2. **Update selectors**: Based on the inspector output, update CSS selectors in `scraper.py` (lines 52-75)
3. **Test the scraper**: Run `python scraper.py` to test scraping
4. **Start collecting data**: Use `python main.py scrape` to begin building your database
5. **Analyze**: Once you have data, use the analysis commands to discover trends

## Notes

- The scraper includes a default 1-second delay between requests to be respectful to the WDR server
- Duplicate songs (same artist, title, and timestamp) are automatically skipped
- All dates should be in YYYY-MM-DD format

## Requirements

- Python >= 3.12
- requests
- beautifulsoup4
- lxml
- polars
- duckdb

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
