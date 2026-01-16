from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import time


class CosmoPlaylistScraper:
    BASE_URL = "https://www1.wdr.de/radio/cosmo/musik/playlist/index.html"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def fetch_playlist(self, date: Optional[datetime] = None) -> List[Dict[str, str]]:
        """
        Fetch playlist for a specific date.

        Args:
            date: Date to fetch playlist for. If None, fetches today's playlist.

        Returns:
            List of song dictionaries with keys: artist, title, time, date
        """
        if date is None:
            date = datetime.now()

        print(f"  Fetching playlist for {date.strftime('%Y-%m-%d')}...")

        # WDR Cosmo's search returns songs ±30 minutes around the queried time
        # Query every hour to ensure complete coverage with some overlap
        all_songs = []
        seen_songs = set()  # Track (artist, title, time) to avoid duplicates within day

        # Skip future hours when querying today
        is_today = date.date() == datetime.now().date()
        max_hour = datetime.now().hour + 1 if is_today else 24

        for hour in range(max_hour):
            print(f"    Querying {hour:02d}:00 (±30 min window)...", end='', flush=True)
            songs = self._fetch_playlist_for_time(date, hour, 0)

            # Deduplicate within this fetch
            new_songs = 0
            for song in songs:
                song_key = (song['artist'], song['title'], song['time'])
                if song_key not in seen_songs:
                    seen_songs.add(song_key)
                    all_songs.append(song)
                    new_songs += 1

            print(f" found {len(songs)} songs ({new_songs} new)")

        # Sort by datetime
        all_songs.sort(key=lambda s: s.get('datetime', ''))

        if all_songs:
            # Log the time range
            first_time = all_songs[0].get('time', 'N/A')
            last_time = all_songs[-1].get('time', 'N/A')
            print(f"  Total unique songs: {len(all_songs)}")
            print(f"  Time range: {first_time} - {last_time}")

        return all_songs

    def _fetch_playlist_for_time(
        self,
        date: datetime,
        hour: int,
        minute: int
    ) -> List[Dict[str, str]]:
        """Fetch playlist for a specific date and time."""
        url = "https://www1.wdr.de/radio/cosmo/musik/playlist/index.jsp"

        form_data = {
            'playlistSearch_date': date.strftime('%Y-%m-%d'),
            'playlistSearch_hours': f'{hour:02d}',
            'playlistSearch_minutes': f'{minute:02d}',
            'submit': 'suchen'
        }

        try:
            response = self.session.post(url, data=form_data, timeout=30)
            response.raise_for_status()

            songs = self._parse_playlist(response.text, date)

            time.sleep(self.delay)

            return songs

        except requests.RequestException as e:
            print(f"  Error fetching playlist for {date} at {hour:02d}:{minute:02d}: {e}")
            return []

    def _parse_playlist(self, html: str, date: datetime) -> List[Dict[str, str]]:
        """
        Parse HTML and extract song information.

        The WDR Cosmo playlist is structured as a table with:
        - Rows: tr.data
        - Datetime: th.entry.datetime (format: "DD.MM.YYYY,<br>HH.MM Uhr")
        - Title: td.entry.title
        - Artist: td.entry.performer
        """
        soup = BeautifulSoup(html, 'lxml')
        songs = []

        # Find the playlist table
        table = soup.select_one('table.thleft')
        if not table:
            print("Warning: Could not find playlist table")
            return songs

        # Get all data rows
        song_rows = table.select('tr.data')

        for row in song_rows:
            try:
                # Extract datetime
                datetime_elem = row.select_one('th.entry.datetime')
                # Extract title
                title_elem = row.select_one('td.entry.title')
                # Extract artist/performer
                artist_elem = row.select_one('td.entry.performer')

                if title_elem and artist_elem:
                    # Parse the datetime text (format: "13.01.2026,<br>17.15 Uhr")
                    datetime_text = datetime_elem.get_text(strip=True) if datetime_elem else ''
                    # Clean up: remove "Uhr" and extract time
                    datetime_text = datetime_text.replace('Uhr', '').strip()

                    # Extract just the time part (e.g., "17.15" from "13.01.2026,17.15")
                    timestamp_str = ''
                    if ',' in datetime_text:
                        time_part = datetime_text.split(',')[1].strip()
                        # Convert "17.15" to "17:15"
                        timestamp_str = time_part.replace('.', ':')

                    song_data = {
                        'artist': artist_elem.get_text(strip=True),
                        'title': title_elem.get_text(strip=True),
                        'time': timestamp_str,
                        'date': date.strftime("%Y-%m-%d"),
                        'datetime': self._parse_timestamp(timestamp_str, date)
                    }
                    songs.append(song_data)

            except Exception as e:
                print(f"Error parsing song row: {e}")
                continue

        return songs

    def _parse_timestamp(self, timestamp_str: str, date: datetime) -> str:
        """
        Convert timestamp string to full datetime.

        Args:
            timestamp_str: Time string like "14:35" or "2:35 PM"
            date: Date for this timestamp

        Returns:
            ISO format datetime string
        """
        if not timestamp_str:
            return ''

        try:
            # Handle common time formats
            for fmt in ['%H:%M', '%I:%M %p', '%H:%M:%S']:
                try:
                    time_obj = datetime.strptime(timestamp_str, fmt)
                    full_datetime = date.replace(
                        hour=time_obj.hour,
                        minute=time_obj.minute,
                        second=time_obj.second
                    )
                    return full_datetime.isoformat()
                except ValueError:
                    continue
        except Exception as e:
            print(f"Error parsing timestamp {timestamp_str}: {e}")

        return ''

    def fetch_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, str]]:
        """
        Fetch playlists for a range of dates.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of all songs across the date range
        """
        all_songs = []
        current_date = start_date

        while current_date <= end_date:
            songs = self.fetch_playlist(current_date)
            all_songs.extend(songs)
            print(f"  Found {len(songs)} songs total for this day\n")

            current_date += timedelta(days=1)

        return all_songs
