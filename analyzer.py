import polars as pl
import duckdb
from datetime import datetime, timedelta
from typing import List, Dict, Optional


class PlaylistAnalyzer:
    def __init__(self, db_path: str = "cosmo_playlist.duckdb"):
        self.db_path = db_path

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a DuckDB connection."""
        return duckdb.connect(self.db_path, read_only=True)

    def top_songs_by_day(self, date: str, top_n: int = 10) -> pl.DataFrame:
        """
        Get most played songs for a specific day.

        Args:
            date: Date string in format 'YYYY-MM-DD'
            top_n: Number of top songs to return

        Returns:
            DataFrame with columns: artist, title, play_count
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT artist, title, COUNT(*) as play_count
            FROM songs
            WHERE date = ?
            GROUP BY artist, title
            ORDER BY play_count DESC
            LIMIT ?
        """, [date, top_n]).pl()

        conn.close()
        return result

    def top_songs_by_week(
        self,
        start_date: str,
        top_n: int = 10
    ) -> pl.DataFrame:
        """
        Get most played songs for a week starting from start_date.

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            top_n: Number of top songs to return

        Returns:
            DataFrame with columns: artist, title, play_count
        """
        conn = self._get_connection()
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = start + timedelta(days=7)

        result = conn.execute("""
            SELECT artist, title, COUNT(*) as play_count
            FROM songs
            WHERE date >= ? AND date < ?
            GROUP BY artist, title
            ORDER BY play_count DESC
            LIMIT ?
        """, [start, end, top_n]).pl()

        conn.close()
        return result

    def top_songs_by_month(
        self,
        year: int,
        month: int,
        top_n: int = 10
    ) -> pl.DataFrame:
        """
        Get most played songs for a specific month.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            top_n: Number of top songs to return

        Returns:
            DataFrame with columns: artist, title, play_count
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT artist, title, COUNT(*) as play_count
            FROM songs
            WHERE EXTRACT(YEAR FROM date) = ?
              AND EXTRACT(MONTH FROM date) = ?
            GROUP BY artist, title
            ORDER BY play_count DESC
            LIMIT ?
        """, [year, month, top_n]).pl()

        conn.close()
        return result

    def top_songs_by_date_range(
        self,
        start_date: str,
        end_date: str,
        top_n: int = 10
    ) -> pl.DataFrame:
        """
        Get most played songs for a custom date range.

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD' (inclusive)
            top_n: Number of top songs to return

        Returns:
            DataFrame with columns: artist, title, play_count
        """
        conn = self._get_connection()
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date() + timedelta(days=1)

        result = conn.execute("""
            SELECT artist, title, COUNT(*) as play_count
            FROM songs
            WHERE date >= ? AND date < ?
            GROUP BY artist, title
            ORDER BY play_count DESC
            LIMIT ?
        """, [start, end, top_n]).pl()

        conn.close()
        return result

    def top_songs(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        top_n: int = 10
    ) -> pl.DataFrame:
        """
        Get most played songs across all time or within a date range.

        Args:
            start_date: Optional start date in format 'YYYY-MM-DD'
            end_date: Optional end date in format 'YYYY-MM-DD'
            top_n: Number of top songs to return

        Returns:
            DataFrame with columns: artist, title, play_count
        """
        conn = self._get_connection()

        where_clauses = []
        params = []

        if start_date:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            where_clauses.append("date >= ?")
            params.append(start)

        if end_date:
            end = datetime.strptime(end_date, '%Y-%m-%d').date() + timedelta(days=1)
            where_clauses.append("date < ?")
            params.append(end)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        params.append(top_n)

        result = conn.execute(f"""
            SELECT artist, title, COUNT(*) as play_count
            FROM songs
            {where_sql}
            GROUP BY artist, title
            ORDER BY play_count DESC
            LIMIT ?
        """, params).pl()

        conn.close()
        return result

    def top_artists(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        top_n: int = 10
    ) -> pl.DataFrame:
        """
        Get most played artists.

        Args:
            start_date: Optional start date in format 'YYYY-MM-DD'
            end_date: Optional end date in format 'YYYY-MM-DD'
            top_n: Number of top artists to return

        Returns:
            DataFrame with columns: artist, play_count
        """
        conn = self._get_connection()

        where_clauses = []
        params = []

        if start_date:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            where_clauses.append("date >= ?")
            params.append(start)

        if end_date:
            end = datetime.strptime(end_date, '%Y-%m-%d').date() + timedelta(days=1)
            where_clauses.append("date < ?")
            params.append(end)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        params.append(top_n)

        result = conn.execute(f"""
            SELECT artist, COUNT(*) as play_count
            FROM songs
            {where_sql}
            GROUP BY artist
            ORDER BY play_count DESC
            LIMIT ?
        """, params).pl()

        conn.close()
        return result

    def get_statistics(self) -> Dict[str, any]:
        """
        Get overall statistics about the playlist data.

        Returns:
            Dictionary with various statistics
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT
                COUNT(*) as total_songs,
                COUNT(DISTINCT artist || '|' || title) as unique_songs,
                COUNT(DISTINCT artist) as unique_artists,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM songs
        """).fetchone()

        if not result or result[0] == 0:
            conn.close()
            return {
                'total_songs': 0,
                'unique_songs': 0,
                'unique_artists': 0,
                'date_range': None
            }

        earliest = result[3]
        latest = result[4]
        days = (latest - earliest).days + 1 if earliest and latest else 0

        stats = {
            'total_songs': result[0],
            'unique_songs': result[1],
            'unique_artists': result[2],
            'earliest_date': earliest.strftime('%Y-%m-%d') if earliest else None,
            'latest_date': latest.strftime('%Y-%m-%d') if latest else None,
            'days_covered': days
        }

        conn.close()
        return stats


def test_analyzer():
    """Test analyzer functionality."""
    analyzer = PlaylistAnalyzer("test_playlist.duckdb")

    stats = analyzer.get_statistics()
    print("Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    test_analyzer()
