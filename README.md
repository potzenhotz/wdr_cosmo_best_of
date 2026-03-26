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
- **Genre Enrichment**: Enrich songs with genre information from Last.fm
- **Spotify Integration**: Link songs to Spotify and export playlists

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
uv run python inspect_playlist.py
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
uv run python main.py scrape
```

Scrape a specific date:
```bash
uv run python main.py scrape --date 2024-01-15
```

Scrape a date range:
```bash
uv run python main.py scrape --start-date 2024-01-01 --end-date 2024-01-31
```

Scrape the last N days:
```bash
uv run python main.py scrape --days 7
```

### Analyze Data

Top songs for a specific day:
```bash
uv run python main.py top-day 2024-01-15
```

Top songs for a week (starting from specified date):
```bash
uv run python main.py top-week 2024-01-15
```

Top songs for a month:
```bash
uv run python main.py top-month 2024 1
```

Top songs for a custom date range:
```bash
uv run python main.py top-range 2024-01-01 2024-01-31
```

Top songs of all time:
```bash
uv run python main.py top-songs
```

Top songs with date filter:
```bash
uv run python main.py top-songs --start-date 2024-01-01 --end-date 2024-01-31
```

Top artists (all time):
```bash
uv run python main.py top-artists
```

Top artists for a date range:
```bash
uv run python main.py top-artists --start-date 2024-01-01 --end-date 2024-01-31
```

Show database statistics:
```bash
uv run python main.py stats
```

### Spotify Setup

To use genre enrichment and playlist export, you need Spotify API credentials:

1. Go to https://developer.spotify.com/dashboard
2. Create a new app
3. Copy the Client ID and Client Secret
4. Add to your `.env` file:

```bash
cp .env.example .env
# Edit .env and add your credentials:
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8888/callback
```

The first time you run a Spotify command, it will open your browser for authentication.

### Enrich with Genre Information

Add genre information (via Last.fm) and Spotify track IDs to your songs. Spotify is used for track matching (playlist export), Last.fm for genres (Spotify deprecated genre data in their API).

```bash
# Enrich all songs without genre
uv run python main.py enrich-genres

# Test with only 10 songs
uv run python main.py enrich-genres --limit 10

# Skip confirmation prompt
uv run python main.py enrich-genres -y

# Show verbose output (including songs not found)
uv run python main.py enrich-genres -v

# Retry songs previously marked as NOT_FOUND
uv run python main.py enrich-genres --retry
```

This stores both the genre (from Last.fm) and the Spotify track ID for each song, enabling playlist export. Spotify credentials are optional — without them, only genre enrichment runs.

### Export to Spotify Playlist

Create Spotify playlists from your top songs:

```bash
# Export all-time top 50 songs
uv run python main.py export-playlist --top --limit 50

# Export top songs of a specific week
uv run python main.py export-playlist --week 2024-01-15 --limit 50

# Export top songs of a month
uv run python main.py export-playlist --month 2024-01 --limit 50

# Export top songs by genre
uv run python main.py export-playlist --genre "hip hop" --limit 30

# Custom playlist name
uv run python main.py export-playlist --top --limit 100 --name "My Cosmo Favorites"

# Skip confirmation prompt
uv run python main.py export-playlist --top -y
```

**Note:** Only songs that were found on Spotify during genre enrichment can be exported to playlists.

### Options

All analysis commands support `--limit` to control the number of results:
```bash
uv run python main.py top-day 2024-01-15 --limit 20
```

Use a custom database file:
```bash
uv run python main.py --database my_playlist.duckdb scrape
uv run python main.py --database my_playlist.duckdb top-day 2024-01-15
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
- `spotify_client.py` - Spotify API integration for genre enrichment and playlist export
- `genre_enricher.py` - Last.fm genre enrichment (legacy fallback)
- `cosmo_playlist.duckdb` - DuckDB database (created automatically)
- `backups/` - Automatic database backups (created before each data modification)
- `.spotify_cache` - Spotify OAuth token cache (created automatically)

## Database Schema

The `songs` table contains:
- `id` - Unique identifier (auto-generated)
- `artist` - Artist name
- `title` - Song title
- `time` - Time of day when played (e.g., "17:15")
- `date` - Date when played (e.g., "2026-01-13")
- `datetime` - Full timestamp (e.g., "2026-01-13T17:15:00")
- `genre` - Genre tags from Spotify (e.g., "hip hop, rap, pop")
- `spotify_track_id` - Spotify track ID for playlist export
- `created_at` - When the record was inserted

**Note:** The `genre` and `spotify_track_id` columns are populated by running the `enrich-genres` command.

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

1. **Run the inspector**: Execute `uv run python inspect_playlist.py` to analyze the page structure
2. **Update selectors**: Based on the inspector output, update CSS selectors in `scraper.py` (lines 52-75)
3. **Test the scraper**: Run `uv run python scraper.py` to test scraping
4. **Start collecting data**: Use `uv run python main.py scrape` to begin building your database
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
- spotipy (for Spotify integration)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
