#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
from scraper import CosmoPlaylistScraper
from database import PlaylistDatabase
from analyzer import PlaylistAnalyzer
from spotify_client import SpotifyClient
from genre_enricher import LastFmGenreEnricher


# Constants
SEPARATOR = "=" * 70
DASH_LINE = "-" * 70


# Helper functions
def _print_ranked_list(data, title: str, limit: int, include_title: bool = True):
    """Print ranked list in standardized format."""
    print(f"\nTop {limit} {title}:")
    print(DASH_LINE)
    for i, row in enumerate(data.iter_rows(named=True), start=1):
        if include_title:
            print(f"{i:2d}. {row['artist']} - {row['title']}")
        else:
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
    _print_ranked_list(top_songs, f"songs on {args.date}", args.limit)


def cmd_top_week(args):
    """Show top songs for a week."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_week(args.start_date, args.limit)

    end_date = datetime.strptime(args.start_date, "%Y-%m-%d") + timedelta(days=6)
    _print_ranked_list(top_songs, f"songs for week {args.start_date} to {end_date.strftime('%Y-%m-%d')}", args.limit)


def cmd_top_month(args):
    """Show top songs for a month."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_month(args.year, args.month, args.limit)
    _print_ranked_list(top_songs, f"songs for {args.year}-{args.month:02d}", args.limit)


def cmd_top_range(args):
    """Show top songs for a date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs_by_date_range(
        args.start_date,
        args.end_date,
        args.limit
    )
    _print_ranked_list(top_songs, f"songs from {args.start_date} to {args.end_date}", args.limit)


def cmd_top_artists(args):
    """Show top artists."""
    analyzer = PlaylistAnalyzer(args.database)
    top_artists = analyzer.top_artists(
        args.start_date,
        args.end_date,
        args.limit
    )
    date_info = _format_date_range(args.start_date, args.end_date, default_text="")
    _print_ranked_list(top_artists, f"artists{date_info}", args.limit, include_title=False)


def cmd_top_songs(args):
    """Show top songs across all time or within date range."""
    analyzer = PlaylistAnalyzer(args.database)
    top_songs = analyzer.top_songs(
        args.start_date,
        args.end_date,
        args.limit
    )
    date_info = _format_date_range(args.start_date, args.end_date)
    _print_ranked_list(top_songs, f"songs{date_info}", args.limit)


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
    """Enrich songs with genre info (Last.fm) and Spotify track IDs."""
    import os
    from dotenv import load_dotenv

    # Load .env file
    load_dotenv()

    db = PlaylistDatabase(args.database)

    # Check if there are any songs in the database
    total_songs = db.get_total_songs()
    if total_songs == 0:
        print("No songs in database! Please run 'scrape' command first.")
        db.close()
        return

    # Initialize Last.fm (required for genres)
    api_key = os.environ.get('LASTFM_API_KEY')
    if not api_key:
        print("ERROR: LASTFM_API_KEY not found!")
        print("Get a free API key at: https://www.last.fm/api/account/create")
        print("\nThen add it to your .env file:")
        print("  LASTFM_API_KEY=your_api_key")
        db.close()
        return

    lastfm = LastFmGenreEnricher(api_key=api_key, verbose=args.verbose)

    # Initialize Spotify (optional, for track IDs)
    spotify = None
    try:
        spotify = SpotifyClient(verbose=args.verbose)
        print("Spotify connected - will also store track IDs for playlist export")
    except ValueError as e:
        print(f"Spotify not available ({e})")
        print("Continuing with Last.fm for genres only (no playlist export)\n")

    # Get songs without genre (or Spotify ID for retry)
    if args.retry:
        print("Finding songs to retry (including NOT_FOUND)...")
        songs = db.get_songs_without_genre(limit=args.limit, include_not_found=True)
        not_found_count = db.get_not_found_count()
        print(f"  ({not_found_count} previously marked as NOT_FOUND)")
    else:
        print("Finding songs without genre information...")
        songs = db.get_songs_without_genre(limit=args.limit)

    if not songs:
        not_found_count = db.get_not_found_count()
        if not_found_count > 0:
            print(f"All new songs have been processed! ({not_found_count} marked as NOT_FOUND)")
            print("Use --retry to retry NOT_FOUND songs.")
        else:
            print("All songs already have genre information!")
        db.close()
        return

    print(f"Found {len(songs)} unique songs to process")
    print(f"Using Last.fm for genres{', Spotify for track IDs' if spotify else ''}")
    print(f"Note: Last.fm rate limit is 5 requests/second")
    est_seconds = len(songs) // 5
    print(f"Estimated time: ~{est_seconds} seconds (~{est_seconds // 60} minutes)\n")

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

    genre_found = 0
    spotify_found = 0
    not_found_count = 0

    for i, song in enumerate(songs, 1):
        artist = song['artist']
        title = song['title']

        # Get genre from Last.fm
        genre = lastfm.lookup_genre(artist, title)

        # Get Spotify track ID (if Spotify is available)
        spotify_id = None
        if spotify:
            track = spotify.search_track(artist, title)
            if track:
                spotify_id = track['track_id']

        if genre:
            genre_found += 1
            if spotify_id:
                spotify_found += 1
            print(f"[{i}/{len(songs)}] ✓ {artist} - {title}")
            print(f"         Genre: {genre}{' | Spotify ✓' if spotify_id else ''}")
            db.update_genre_and_spotify_id(artist, title, genre, spotify_id, skip_backup=True)
        else:
            not_found_count += 1
            if spotify_id:
                spotify_found += 1
                # No genre but have Spotify ID - store it with NOT_FOUND genre
                db.update_genre_and_spotify_id(artist, title, "NOT_FOUND", spotify_id, skip_backup=True)
            else:
                db.update_genre_and_spotify_id(artist, title, "NOT_FOUND", None, skip_backup=True)
            if args.verbose:
                print(f"[{i}/{len(songs)}] ✗ {artist} - {title} (no genre{' | Spotify ✓' if spotify_id else ''})")

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
    print(f"  Genres found (Last.fm): {genre_found}")
    print(f"  Spotify track IDs:      {spotify_found}")
    print(f"  Not found:              {not_found_count}")
    print(f"  Total processed:        {len(songs)}")

    db.close()


def cmd_export_playlist(args):
    """Export top songs to a Spotify playlist."""
    from dotenv import load_dotenv

    load_dotenv()

    db = PlaylistDatabase(args.database)

    # Initialize Spotify client
    try:
        spotify = SpotifyClient(verbose=args.verbose if hasattr(args, 'verbose') else False)
    except ValueError as e:
        print(f"ERROR: {e}")
        db.close()
        return

    # Determine playlist name and query parameters
    playlist_name = args.name
    description = "Generated from WDR Cosmo playlist data"

    if args.week:
        week_start = args.week
        week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
        if not playlist_name:
            playlist_name = f"Cosmo Week {week_start}"
        description = f"Top songs from WDR Cosmo, week of {week_start} to {week_end}"
        songs = db.get_top_songs_with_spotify_ids(limit=args.limit, week_start=week_start)
    elif args.month:
        if not playlist_name:
            playlist_name = f"Cosmo {args.month}"
        description = f"Top songs from WDR Cosmo, {args.month}"
        songs = db.get_top_songs_with_spotify_ids(limit=args.limit, month=args.month)
    elif args.genre:
        if not playlist_name:
            playlist_name = f"Cosmo {args.genre.title()}"
        description = f"Top {args.genre} songs from WDR Cosmo"
        songs = db.get_top_songs_with_spotify_ids(limit=args.limit, genre_filter=args.genre)
    elif args.top:
        if not playlist_name:
            playlist_name = "Cosmo All-Time Top"
        description = "All-time top songs from WDR Cosmo"
        songs = db.get_top_songs_with_spotify_ids(limit=args.limit)
    else:
        print("ERROR: Please specify one of: --week, --month, --genre, or --top")
        db.close()
        return

    if not songs:
        print("No songs found with Spotify IDs matching your criteria.")
        print("Run 'enrich-genres' first to link songs to Spotify.")
        db.close()
        return

    print(f"Creating playlist: {playlist_name}")
    print(f"Songs to add: {len(songs)}")
    print(DASH_LINE)

    for i, song in enumerate(songs[:10], 1):
        print(f"  {i}. {song['artist']} - {song['title']} ({song['play_count']} plays)")

    if len(songs) > 10:
        print(f"  ... and {len(songs) - 10} more")

    print()

    if not args.yes:
        response = input("Create this playlist on Spotify? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("Aborted.")
            db.close()
            return

    # Extract track IDs and create playlist
    track_ids = [song['spotify_track_id'] for song in songs]

    try:
        playlist_url = spotify.create_playlist(playlist_name, track_ids, description)
        print(f"\n✓ Playlist created successfully!")
        print(f"  URL: {playlist_url}")
        print(f"  Songs added: {len(track_ids)}")
    except Exception as e:
        print(f"\nERROR creating playlist: {e}")

    db.close()


def cmd_clear_genres(args):
    """Clear all genre information for a fresh start."""
    db = PlaylistDatabase(args.database)

    total_songs = db.get_total_songs()
    if total_songs == 0:
        print("No songs in database!")
        db.close()
        return

    print(f"This will clear genre information from ALL {total_songs} songs.")
    print("A backup will be created before clearing.\n")

    if not args.yes:
        response = input("Proceed? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("Aborted.")
            db.close()
            return

    db.clear_all_genres()
    print("Done! You can now run 'enrich-genres' to fetch fresh genre data.")
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
        help="Enrich songs with genre information from Spotify"
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
    enrich_parser.add_argument(
        "--retry",
        action="store_true",
        help="Retry songs previously marked as NOT_FOUND"
    )
    enrich_parser.set_defaults(func=cmd_enrich_genres)

    clear_parser = subparsers.add_parser(
        "clear-genres",
        help="Clear all genre information for a fresh start"
    )
    clear_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )
    clear_parser.set_defaults(func=cmd_clear_genres)

    export_parser = subparsers.add_parser(
        "export-playlist",
        help="Export top songs to a Spotify playlist"
    )
    export_group = export_parser.add_mutually_exclusive_group()
    export_group.add_argument(
        "--week",
        help="Export top songs for a week (YYYY-MM-DD start date)"
    )
    export_group.add_argument(
        "--month",
        help="Export top songs for a month (YYYY-MM)"
    )
    export_group.add_argument(
        "--genre",
        help="Export top songs by genre"
    )
    export_group.add_argument(
        "--top",
        action="store_true",
        help="Export all-time top songs"
    )
    export_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of songs to include (default: 50)"
    )
    export_parser.add_argument(
        "--name",
        help="Custom playlist name"
    )
    export_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )
    export_parser.set_defaults(func=cmd_export_playlist)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
