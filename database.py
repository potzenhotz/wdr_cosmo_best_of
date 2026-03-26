import duckdb
import shutil
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path


class PlaylistDatabase:
    def __init__(self, db_path: str = "cosmo_playlist.duckdb"):
        self.db_path = db_path
        self.conn = None
        self.backup_dir = Path("backups")
        self.backup_dir.mkdir(exist_ok=True)
        self._init_database()

    def _init_database(self):
        """Initialize database and create tables if they don't exist."""
        self.conn = duckdb.connect(self.db_path)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id BIGINT,
                artist VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                time VARCHAR,
                date DATE NOT NULL,
                datetime TIMESTAMP,
                genre VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(artist, title, datetime)
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_artist ON songs(artist)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date ON songs(date)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_datetime ON songs(datetime)
        """)

        # Migration: Add spotify_track_id column if it doesn't exist
        self._migrate_add_spotify_track_id()

    def _migrate_add_spotify_track_id(self):
        """Add spotify_track_id column if it doesn't exist."""
        try:
            # Check if column exists
            result = self.conn.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'songs' AND column_name = 'spotify_track_id'
            """).fetchone()

            if not result:
                self.conn.execute("ALTER TABLE songs ADD COLUMN spotify_track_id VARCHAR")
                print("Database migration: Added spotify_track_id column")
        except Exception as e:
            # Column might already exist or other error
            pass

    def _create_backup(self, operation_name: str = "") -> Optional[Path]:
        """
        Create a backup of the database.

        Args:
            operation_name: Name of the operation being performed (for backup filename)

        Returns:
            Path to the backup file or None if backup failed
        """
        if not Path(self.db_path).exists():
            print("  Warning: Database file doesn't exist yet, skipping backup")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"{Path(self.db_path).stem}_backup_{timestamp}.duckdb"

        try:
            shutil.copy2(self.db_path, backup_path)
            print(f"Backup created: {backup_path}")
            return backup_path
        except Exception as e:
            print(f"  WARNING: Failed to create backup: {e}")
            return None

    def _get_row_count(self) -> int:
        """Get current row count in songs table."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM songs").fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    def _verify_data_integrity(self, expected_min_rows: int, operation_name: str) -> bool:
        """
        Verify that the database still has data after an operation.

        Args:
            expected_min_rows: Minimum expected row count
            operation_name: Name of the operation for error messages

        Returns:
            True if data is intact, False if data was lost
        """
        try:
            current_count = self.get_total_songs()
            if current_count == 0 and expected_min_rows > 0:
                print(f"\n⚠️  WARNING: Data loss detected during {operation_name}!")
                print(f"   Expected at least: {expected_min_rows} songs")
                print(f"   Found:            0 songs")
                return False
            elif current_count < expected_min_rows:
                print(f"\n⚠️  WARNING: Data loss detected during {operation_name}!")
                print(f"   Expected at least: {expected_min_rows} songs")
                print(f"   Found:            {current_count} songs")
                print(f"   Missing:          {expected_min_rows - current_count} songs")
                return False

            return True

        except Exception as e:
            print(f"  ERROR during verification: {e}")
            return False

    def insert_songs(self, songs: List[Dict[str, str]]) -> int:
        """
        Insert multiple songs into the database.
        Creates backup before insertion and verifies data integrity after.

        Args:
            songs: List of song dictionaries

        Returns:
            Number of songs inserted (excluding duplicates)
        """
        # Create backup before modifying data
        rows_before = self._get_row_count()
        backup_path = self._create_backup("insert_songs")

        inserted = 0
        skipped = 0

        # Get the current max ID
        result = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM songs").fetchone()
        next_id = result[0] + 1

        for i, song in enumerate(songs):
            try:
                # Cast string dates to proper types for DuckDB
                date_str = song.get('date', '')
                datetime_str = song.get('datetime', '')

                self.conn.execute("""
                    INSERT INTO songs (id, artist, title, time, date, datetime)
                    VALUES (?, ?, ?, ?, CAST(? AS DATE), CAST(? AS TIMESTAMP))
                """, [
                    next_id,
                    song.get('artist', ''),
                    song.get('title', ''),
                    song.get('time', ''),
                    date_str if date_str else None,
                    datetime_str if datetime_str else None
                ])
                inserted += 1
                next_id += 1
            except duckdb.ConstraintException:
                # Duplicate song - skip
                skipped += 1
                continue
            except Exception as e:
                print(f"  ERROR inserting song {song.get('artist', '')} - {song.get('title', '')}: {e}")
                continue

        # Verify data integrity after insertion
        self._verify_data_integrity(rows_before, "insert_songs")

        return inserted

    def get_songs_by_date(self, date: str) -> List[Dict]:
        """Get all songs for a specific date."""
        result = self.conn.execute("""
            SELECT * FROM songs
            WHERE date = ?
            ORDER BY datetime
        """, [date]).fetchall()

        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]

    def get_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Get the earliest and latest dates in the database."""
        result = self.conn.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM songs
        """).fetchone()

        if result:
            return str(result[0]) if result[0] else None, str(result[1]) if result[1] else None
        return None, None

    def get_total_songs(self) -> int:
        """Get total number of songs in database."""
        result = self.conn.execute("SELECT COUNT(*) as count FROM songs").fetchone()
        return result[0] if result else 0

    def update_genre(self, artist: str, title: str, genre: str, skip_backup: bool = False) -> int:
        """
        Update genre for all occurrences of a song.

        Args:
            artist: Artist name
            title: Song title
            genre: Genre to set
            skip_backup: If True, skip backup (useful for batch operations)

        Returns:
            Number of rows updated
        """
        # Create backup before modifying data (unless explicitly skipped for batch operations)
        if not skip_backup:
            rows_before = self._get_row_count()
            backup_path = self._create_backup("update_genre")

        result = self.conn.execute("""
            UPDATE songs
            SET genre = ?
            WHERE artist = ? AND title = ?
        """, [genre, artist, title])

        # Verify data integrity after update
        if not skip_backup:
            self._verify_data_integrity(rows_before, "update_genre")

        return result.fetchone()[0] if result else 0

    def get_songs_without_genre(self, limit: Optional[int] = None, include_not_found: bool = False) -> List[Dict]:
        """
        Get distinct songs that don't have genre information.

        Args:
            limit: Optional limit on number of songs to return
            include_not_found: If True, also include songs marked as NOT_FOUND (for retry)

        Returns:
            List of dictionaries with artist and title
        """
        if include_not_found:
            # Include both NULL (never tried) and NOT_FOUND (retry)
            where_clause = "WHERE genre IS NULL OR genre = 'NOT_FOUND'"
        else:
            # Only songs never tried
            where_clause = "WHERE genre IS NULL"

        query = f"""
            SELECT DISTINCT artist, title
            FROM songs
            {where_clause}
            ORDER BY artist, title
        """

        if limit:
            query += f" LIMIT {limit}"

        result = self.conn.execute(query).fetchall()
        columns = ['artist', 'title']
        return [dict(zip(columns, row)) for row in result]

    def get_not_found_count(self) -> int:
        """Get count of songs marked as NOT_FOUND."""
        result = self.conn.execute(
            "SELECT COUNT(DISTINCT artist || '|||' || title) FROM songs WHERE genre = 'NOT_FOUND'"
        ).fetchone()
        return result[0] if result else 0

    def clear_all_genres(self) -> int:
        """
        Clear all genre information for a fresh start.

        Returns:
            Number of rows updated
        """
        self._create_backup("clear_genres")

        # Count rows with genre before clearing
        count = self.conn.execute("SELECT COUNT(*) FROM songs WHERE genre IS NOT NULL").fetchone()[0]

        self.conn.execute("UPDATE songs SET genre = NULL")
        print(f"Cleared genres from {count} songs")
        return count

    def update_spotify_track_id(self, artist: str, title: str, spotify_track_id: str, skip_backup: bool = False) -> int:
        """
        Update Spotify track ID for all occurrences of a song.

        Args:
            artist: Artist name
            title: Song title
            spotify_track_id: Spotify track ID
            skip_backup: If True, skip backup (useful for batch operations)

        Returns:
            Number of rows updated
        """
        if not skip_backup:
            rows_before = self._get_row_count()
            self._create_backup("update_spotify_track_id")

        result = self.conn.execute("""
            UPDATE songs
            SET spotify_track_id = ?
            WHERE artist = ? AND title = ?
        """, [spotify_track_id, artist, title])

        if not skip_backup:
            self._verify_data_integrity(rows_before, "update_spotify_track_id")

        return result.fetchone()[0] if result else 0

    def update_genre_and_spotify_id(self, artist: str, title: str, genre: str, spotify_track_id: Optional[str], skip_backup: bool = False) -> int:
        """
        Update both genre and Spotify track ID for all occurrences of a song.

        Args:
            artist: Artist name
            title: Song title
            genre: Genre to set
            spotify_track_id: Spotify track ID (can be None)
            skip_backup: If True, skip backup (useful for batch operations)

        Returns:
            Number of rows updated
        """
        if not skip_backup:
            rows_before = self._get_row_count()
            self._create_backup("update_genre_and_spotify_id")

        result = self.conn.execute("""
            UPDATE songs
            SET genre = ?, spotify_track_id = ?
            WHERE artist = ? AND title = ?
        """, [genre, spotify_track_id, artist, title])

        if not skip_backup:
            self._verify_data_integrity(rows_before, "update_genre_and_spotify_id")

        return result.fetchone()[0] if result else 0

    def get_top_songs_with_spotify_ids(
        self,
        limit: int = 50,
        week_start: Optional[str] = None,
        month: Optional[str] = None,
        genre_filter: Optional[str] = None
    ) -> List[Dict]:
        """
        Get top songs that have Spotify track IDs for playlist export.

        Args:
            limit: Maximum number of songs to return
            week_start: Week start date (YYYY-MM-DD) for weekly filter
            month: Month (YYYY-MM) for monthly filter
            genre_filter: Filter by genre (substring match)

        Returns:
            List of dicts with artist, title, play_count, spotify_track_id
        """
        conditions = ["spotify_track_id IS NOT NULL"]
        params = []

        if week_start:
            # Week is 7 days starting from week_start
            conditions.append("date >= CAST(? AS DATE)")
            conditions.append("date < CAST(? AS DATE) + INTERVAL 7 DAY")
            params.extend([week_start, week_start])
        elif month:
            # Month filter (YYYY-MM format)
            conditions.append("strftime('%Y-%m', date) = ?")
            params.append(month)

        if genre_filter:
            conditions.append("LOWER(genre) LIKE ?")
            params.append(f"%{genre_filter.lower()}%")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT artist, title, COUNT(*) as play_count, spotify_track_id
            FROM songs
            WHERE {where_clause}
            GROUP BY artist, title, spotify_track_id
            ORDER BY play_count DESC
            LIMIT ?
        """
        params.append(limit)

        result = self.conn.execute(query, params).fetchall()
        columns = ['artist', 'title', 'play_count', 'spotify_track_id']
        return [dict(zip(columns, row)) for row in result]

    def get_songs_with_spotify_ids_count(self) -> int:
        """Get count of unique songs with Spotify track IDs."""
        result = self.conn.execute(
            "SELECT COUNT(DISTINCT artist || '|||' || title) FROM songs WHERE spotify_track_id IS NOT NULL"
        ).fetchone()
        return result[0] if result else 0

    def get_songs_without_spotify_id(self, limit: Optional[int] = None, include_not_found: bool = False) -> List[Dict]:
        """
        Get distinct songs that don't have Spotify track ID.

        Args:
            limit: Optional limit on number of songs to return
            include_not_found: If True, also include songs marked as NOT_FOUND genre (for retry)

        Returns:
            List of dictionaries with artist and title
        """
        if include_not_found:
            where_clause = "WHERE spotify_track_id IS NULL"
        else:
            where_clause = "WHERE spotify_track_id IS NULL AND (genre IS NULL OR genre != 'NOT_FOUND')"

        query = f"""
            SELECT DISTINCT artist, title
            FROM songs
            {where_clause}
            ORDER BY artist, title
        """

        if limit:
            query += f" LIMIT {limit}"

        result = self.conn.execute(query).fetchall()
        columns = ['artist', 'title']
        return [dict(zip(columns, row)) for row in result]

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
