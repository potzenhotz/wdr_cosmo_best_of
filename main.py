#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from scraper import CosmoPlaylistScraper
from database import PlaylistDatabase
from analyzer import PlaylistAnalyzer
from genre_enricher import MusicBrainzGenreEnricher


# Constants
SEPARATOR = "=" * 70
DASH_LINE = "-" * 70


# Helper functions
def _print_top_songs(songs, title: str, limit: int):
    """Print top songs in standardized format."""
    print(f"\nTop {limit} {title}:")
    print(DASH_LINE)
    for i, row in enumerate(songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def _print_top_artists(artists, title: str, limit: int):
    """Print top artists in standardized format."""
    print(f"\nTop {limit} {title}:")
    print(DASH_LINE)
    for i, row in enumerate(artists.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']}")
        print(f"    Played {row['play_count']} times")


def _format_date_range(start_date, end_date, default_text="of all time"):
    """Format date range for display."""
    if start_date and end_date:
        return f" from {start_date} to {end_date}"
    elif start_date:
        return f" from {start_date}"
    elif end_date:
        return f" until {end_date}"
    return f" {default_text}"


def cmd_scrape(args):
    """Scrape playlist data from WDR Cosmo."""
    scraper = CosmoPlaylistScraper(delay=args.delay)
    db = PlaylistDatabase(args.database)

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
        print(f"Scraping playlist for {args.date}...")
        songs = scraper.fetch_playlist(date)
    elif args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        print(f"Scraping playlists from {args.start_date} to {args.end_date}...")
        songs = scraper.fetch_date_range(start, end)
    elif args.days:
        end = datetime.now()
        start = end - timedelta(days=args.days - 1)
        print(f"Scraping last {args.days} days...")
        songs = scraper.fetch_date_range(start, end)
    else:
        print("Scraping today's playlist...")
        songs = scraper.fetch_playlist()

    print(f"\nScraped {len(songs)} songs")

    if songs:
        inserted = db.insert_songs(songs)
        print(f"Inserted {inserted} new songs into database")
        print(f"Skipped {len(songs) - inserted} duplicates")

    db.close()


def cmd_top_day(args):
    """Show top songs for a specific day."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_day(args.date, args.limit)
    _print_top_songs(top_songs, f"songs on {args.date}", args.limit)


def cmd_top_week(args):
    """Show top songs for a week."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_week(args.start_date, args.limit)

    end_date = datetime.strptime(args.start_date, "%Y-%m-%d") + timedelta(days=6)
    _print_top_songs(top_songs, f"songs for week {args.start_date} to {end_date.strftime('%Y-%m-%d')}", args.limit)


def cmd_top_month(args):
    """Show top songs for a month."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_month(args.year, args.month, args.limit)
    _print_top_songs(top_songs, f"songs for {args.year}-{args.month:02d}", args.limit)


def cmd_top_range(args):
    """Show top songs for a date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_date_range(
        args.start_date,
        args.end_date,
        args.limit
    )
    _print_top_songs(top_songs, f"songs from {args.start_date} to {args.end_date}", args.limit)


def cmd_top_artists(args):
    """Show top artists."""
    analyzer = PlaylistAnalyzer(args.database)
    top_artists = analyzer.top_artists(
        args.start_date,
        args.end_date,
        args.limit
    )
    date_info = _format_date_range(args.start_date, args.end_date, default_text="")
    _print_top_artists(top_artists, f"artists{date_info}", args.limit)


def cmd_top_songs(args):
    """Show top songs across all time or within date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs(
        args.start_date,
        args.end_date,
        args.limit
    )
    date_info = _format_date_range(args.start_date, args.end_date)
    _print_top_songs(top_songs, f"songs{date_info}", args.limit)


def cmd_stats(args):
    """Show database statistics."""
    analyzer = PlaylistAnalyzer(args.database)
    stats = analyzer.get_statistics()

    print("\nDatabase Statistics:")
    print("-" * 50)
    print(f"Total songs played:    {stats.get('total_songs', 0):,}")
    print(f"Unique songs:          {stats.get('unique_songs', 0):,}")
    print(f"Unique artists:        {stats.get('unique_artists', 0):,}")

    if stats.get('earliest_date'):
        print(f"Earliest date:         {stats['earliest_date']}")
        print(f"Latest date:           {stats['latest_date']}")
        print(f"Days covered:          {stats['days_covered']}")


def cmd_enrich_genres(args):
    """Enrich songs with genre information from MusicBrainz."""
    db = PlaylistDatabase(args.database)
    enricher = MusicBrainzGenreEnricher()

    # Check if there are any songs in the database
    total_songs = db.get_total_songs()
    if total_songs == 0:
        print("No songs in database! Please run 'scrape' command first.")
        db.close()
        return

    # Get songs without genre
    print("Finding songs without genre information...")
    songs = db.get_songs_without_genre(limit=args.limit)

    if not songs:
        print("All songs already have genre information!")
        db.close()
        return

    print(f"Found {len(songs)} unique songs without genre information")
    print(f"Note: MusicBrainz rate limit is 1 request/second")
    print(f"Estimated time: ~{len(songs)} seconds (~{len(songs) // 60} minutes)\n")

    if not args.yes:
        response = input(f"Proceed with enriching {len(songs)} songs? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("Aborted.")
            db.close()
            return

    # Create a single backup before starting batch genre enrichment
    print(f"\n{SEPARATOR}")
    print("Creating backup before genre enrichment...")
    rows_before = db._get_row_count()
    backup_path = db._create_backup("enrich_genres_batch")

    print("\nEnriching genres...")
    print(SEPARATOR)

    found_count = 0
    not_found_count = 0

    def progress_callback(current, total, artist, title, genre):
        nonlocal found_count, not_found_count

        if genre:
            found_count += 1
            print(f"[{current}/{total}] ✓ {artist} - {title}")
            print(f"         Genre: {genre}")
            # Skip backup for individual updates during batch operation
            db.update_genre(artist, title, genre, skip_backup=True)
        else:
            not_found_count += 1
            if args.verbose:
                print(f"[{current}/{total}] ✗ {artist} - {title} (not found)")

    enricher.enrich_songs(songs, on_progress=progress_callback)

    # Verify data integrity after batch enrichment
    print(f"\n{SEPARATOR}")
    print("Verifying data integrity...")
    if db._verify_data_integrity(rows_before, "enrich_genres_batch"):
        print("✓ Data integrity verified - all songs preserved")
    else:
        print("\n⚠️  DATA LOSS DETECTED!")
        print(f"   Backup available at: {backup_path}")
        print(f"   To restore: cp {backup_path} {args.database}")

    print(f"\n{SEPARATOR}")
    print(f"Enrichment complete!")
    print(f"  Found genres:     {found_count}")
    print(f"  Not found:        {not_found_count}")
    print(f"  Total processed:  {len(songs)}")

    db.close()


def main():
    parser = argparse.ArgumentParser(
        description="WDR Cosmo Playlist Scraper and Analyzer"
    )
    parser.add_argument(
        "--database",
        default="cosmo_playlist.duckdb",
        help="Database file path (default: cosmo_playlist.duckdb)"
    )

    # Common argument parsers
    limit_parser = argparse.ArgumentParser(add_help=False)
    limit_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of results (default: 10)"
    )

    date_range_parser = argparse.ArgumentParser(add_help=False)
    date_range_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    date_range_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    scrape_parser = subparsers.add_parser("scrape", help="Scrape playlist data")
    scrape_parser.add_argument("--date", help="Specific date (YYYY-MM-DD)")
    scrape_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    scrape_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    scrape_parser.add_argument("--days", type=int, help="Last N days")
    scrape_parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between requests in seconds (default: 1.0)"
    )
    scrape_parser.set_defaults(func=cmd_scrape)

    day_parser = subparsers.add_parser(
        "top-day",
        parents=[limit_parser],
        help="Top songs for a day"
    )
    day_parser.add_argument("date", help="Date (YYYY-MM-DD)")
    day_parser.set_defaults(func=cmd_top_day)

    week_parser = subparsers.add_parser(
        "top-week",
        parents=[limit_parser],
        help="Top songs for a week"
    )
    week_parser.add_argument("start_date", help="Week start date (YYYY-MM-DD)")
    week_parser.set_defaults(func=cmd_top_week)

    month_parser = subparsers.add_parser(
        "top-month",
        parents=[limit_parser],
        help="Top songs for a month"
    )
    month_parser.add_argument("year", type=int, help="Year (e.g., 2024)")
    month_parser.add_argument("month", type=int, help="Month (1-12)")
    month_parser.set_defaults(func=cmd_top_month)

    range_parser = subparsers.add_parser(
        "top-range",
        parents=[limit_parser],
        help="Top songs for a date range"
    )
    range_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    range_parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    range_parser.set_defaults(func=cmd_top_range)

    artists_parser = subparsers.add_parser(
        "top-artists",
        parents=[date_range_parser, limit_parser],
        help="Top artists"
    )
    artists_parser.set_defaults(func=cmd_top_artists)

    songs_parser = subparsers.add_parser(
        "top-songs",
        parents=[date_range_parser, limit_parser],
        help="Top songs of all time"
    )
    songs_parser.set_defaults(func=cmd_top_songs)

    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    enrich_parser = subparsers.add_parser(
        "enrich-genres",
        help="Enrich songs with genre information from MusicBrainz"
    )
    enrich_parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of songs to enrich (for testing)"
    )
    enrich_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )
    enrich_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show songs where genre was not found"
    )
    enrich_parser.set_defaults(func=cmd_enrich_genres)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
