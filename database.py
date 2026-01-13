import duckdb
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path


class PlaylistDatabase:
    def __init__(self, db_path: str = "cosmo_playlist.duckdb"):
        self.db_path = db_path
        self.conn = None
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

    def insert_songs(self, songs: List[Dict[str, str]]) -> int:
        """
        Insert multiple songs into the database.

        Args:
            songs: List of song dictionaries

        Returns:
            Number of songs inserted (excluding duplicates)
        """
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

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def test_database():
    """Test database functionality."""
    db = PlaylistDatabase("test_playlist.duckdb")

    test_songs = [
        {
            'artist': 'Test Artist 1',
            'title': 'Test Song 1',
            'time': '14:30',
            'date': '2024-01-15',
            'datetime': '2024-01-15T14:30:00'
        },
        {
            'artist': 'Test Artist 2',
            'title': 'Test Song 2',
            'time': '15:45',
            'date': '2024-01-15',
            'datetime': '2024-01-15T15:45:00'
        }
    ]

    inserted = db.insert_songs(test_songs)
    print(f"Inserted {inserted} songs")

    total = db.get_total_songs()
    print(f"Total songs in database: {total}")

    songs = db.get_songs_by_date('2024-01-15')
    print(f"\nSongs on 2024-01-15:")
    for song in songs:
        print(f"  {song['artist']} - {song['title']} at {song['time']}")

    db.close()

    Path("test_playlist.duckdb").unlink(missing_ok=True)
    print("\nTest database deleted")


if __name__ == "__main__":
    test_database()
