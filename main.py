#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from scraper import CosmoPlaylistScraper
from database import PlaylistDatabase
from analyzer import PlaylistAnalyzer


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

    print(f"\nTop {args.limit} songs on {args.date}:")
    print("-" * 70)
    for i, row in enumerate(top_songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def cmd_top_week(args):
    """Show top songs for a week."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_week(args.start_date, args.limit)

    end_date = datetime.strptime(args.start_date, "%Y-%m-%d") + timedelta(days=6)
    print(f"\nTop {args.limit} songs for week {args.start_date} to {end_date.strftime('%Y-%m-%d')}:")
    print("-" * 70)
    for i, row in enumerate(top_songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def cmd_top_month(args):
    """Show top songs for a month."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_month(args.year, args.month, args.limit)

    print(f"\nTop {args.limit} songs for {args.year}-{args.month:02d}:")
    print("-" * 70)
    for i, row in enumerate(top_songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def cmd_top_range(args):
    """Show top songs for a date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_date_range(
        args.start_date,
        args.end_date,
        args.limit
    )

    print(f"\nTop {args.limit} songs from {args.start_date} to {args.end_date}:")
    print("-" * 70)
    for i, row in enumerate(top_songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def cmd_top_artists(args):
    """Show top artists."""
    analyzer = PlaylistAnalyzer(args.database)
    top_artists = analyzer.top_artists(
        args.start_date,
        args.end_date,
        args.limit
    )

    date_info = ""
    if args.start_date and args.end_date:
        date_info = f" from {args.start_date} to {args.end_date}"
    elif args.start_date:
        date_info = f" from {args.start_date}"
    elif args.end_date:
        date_info = f" until {args.end_date}"

    print(f"\nTop {args.limit} artists{date_info}:")
    print("-" * 70)
    for i, row in enumerate(top_artists.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']}")
        print(f"    Played {row['play_count']} times")


def cmd_top_songs(args):
    """Show top songs across all time or within date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs(
        args.start_date,
        args.end_date,
        args.limit
    )

    date_info = ""
    if args.start_date and args.end_date:
        date_info = f" from {args.start_date} to {args.end_date}"
    elif args.start_date:
        date_info = f" from {args.start_date}"
    elif args.end_date:
        date_info = f" until {args.end_date}"
    else:
        date_info = " of all time"

    print(f"\nTop {args.limit} songs{date_info}:")
    print("-" * 70)
    for i, row in enumerate(top_songs.iter_rows(named=True), start=1):
        print(f"{i:2d}. {row['artist']} - {row['title']}")
        print(f"    Played {row['play_count']} times")


def cmd_stats(args):
    """Show database statistics."""
    analyzer = PlaylistAnalyzer(args.database)
    stats = analyzer.get_statistics()

    print("\nDatabase Statistics:")
    print("-" * 50)
    print(f"Total songs played:    {stats.get('total_songs', 0)}")
    print(f"Unique songs:          {stats.get('unique_songs', 0)}")
    print(f"Unique artists:        {stats.get('unique_artists', 0)}")

    if stats.get('earliest_date'):
        print(f"Earliest date:         {stats['earliest_date']}")
        print(f"Latest date:           {stats['latest_date']}")
        print(f"Days covered:          {stats['days_covered']}")


def main():
    parser = argparse.ArgumentParser(
        description="WDR Cosmo Playlist Scraper and Analyzer"
    )
    parser.add_argument(
        "--database",
        default="cosmo_playlist.duckdb",
        help="Database file path (default: cosmo_playlist.duckdb)"
    )

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

    day_parser = subparsers.add_parser("top-day", help="Top songs for a day")
    day_parser.add_argument("date", help="Date (YYYY-MM-DD)")
    day_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    day_parser.set_defaults(func=cmd_top_day)

    week_parser = subparsers.add_parser("top-week", help="Top songs for a week")
    week_parser.add_argument("start_date", help="Week start date (YYYY-MM-DD)")
    week_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    week_parser.set_defaults(func=cmd_top_week)

    month_parser = subparsers.add_parser("top-month", help="Top songs for a month")
    month_parser.add_argument("year", type=int, help="Year (e.g., 2024)")
    month_parser.add_argument("month", type=int, help="Month (1-12)")
    month_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    month_parser.set_defaults(func=cmd_top_month)

    range_parser = subparsers.add_parser(
        "top-range",
        help="Top songs for a date range"
    )
    range_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    range_parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    range_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    range_parser.set_defaults(func=cmd_top_range)

    artists_parser = subparsers.add_parser("top-artists", help="Top artists")
    artists_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    artists_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    artists_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    artists_parser.set_defaults(func=cmd_top_artists)

    songs_parser = subparsers.add_parser("top-songs", help="Top songs of all time")
    songs_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    songs_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    songs_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    songs_parser.set_defaults(func=cmd_top_songs)

    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
