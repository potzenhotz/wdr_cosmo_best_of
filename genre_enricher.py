"""
Genre enrichment using MusicBrainz API.

Respects MusicBrainz rate limits: 1 request per second.
"""

import requests
import time
from typing import Optional, List
from urllib.parse import quote


class MusicBrainzGenreEnricher:
    BASE_URL = "https://musicbrainz.org/ws/2"
    RATE_LIMIT_DELAY = 1.0  # 1 second between requests

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WDRCosmoAnalyzer/0.1.0 (educational project)',
            'Accept': 'application/json'
        })
        self.last_request_time = 0

    def _rate_limit(self):
        """Ensure we respect MusicBrainz rate limit of 1 request/second."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def lookup_genre(self, artist: str, title: str) -> Optional[str]:
        """
        Look up genre information for a song from MusicBrainz.

        Args:
            artist: Artist name
            title: Song title

        Returns:
            Genre string (comma-separated if multiple) or None if not found
        """
        self._rate_limit()

        try:
            # Search for the recording
            query = f'artist:"{artist}" AND recording:"{title}"'
            url = f"{self.BASE_URL}/recording"

            params = {
                'query': query,
                'limit': 1,
                'fmt': 'json'
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if not data.get('recordings'):
                return None

            recording = data['recordings'][0]

            # Get tags (which are similar to genres in MusicBrainz)
            tags = recording.get('tags', [])

            if not tags:
                return None

            # Sort by count (most common tags first) and take top 3
            sorted_tags = sorted(tags, key=lambda t: t.get('count', 0), reverse=True)
            top_tags = [tag['name'] for tag in sorted_tags[:3]]

            if top_tags:
                return ', '.join(top_tags)

            return None

        except requests.RequestException as e:
            print(f"  Error fetching genre for {artist} - {title}: {e}")
            return None
        except (KeyError, IndexError, ValueError) as e:
            print(f"  Error parsing genre data for {artist} - {title}: {e}")
            return None

    def enrich_songs(
        self,
        songs: List[dict],
        on_progress=None
    ) -> dict:
        """
        Enrich multiple songs with genre information.

        Args:
            songs: List of dicts with 'artist' and 'title' keys
            on_progress: Optional callback function(current, total, artist, title, genre)

        Returns:
            Dictionary with statistics: {
                'total': int,
                'found': int,
                'not_found': int,
                'errors': int
            }
        """
        stats = {
            'total': len(songs),
            'found': 0,
            'not_found': 0,
            'errors': 0
        }

        for i, song in enumerate(songs, 1):
            artist = song['artist']
            title = song['title']

            try:
                genre = self.lookup_genre(artist, title)

                if genre:
                    stats['found'] += 1
                    if on_progress:
                        on_progress(i, len(songs), artist, title, genre)
                else:
                    stats['not_found'] += 1
                    if on_progress:
                        on_progress(i, len(songs), artist, title, None)

            except Exception as e:
                stats['errors'] += 1
                print(f"  Unexpected error for {artist} - {title}: {e}")

        return stats


def test_enricher():
    """Test the genre enricher."""
    enricher = MusicBrainzGenreEnricher()

    # Test with some known songs
    test_songs = [
        {'artist': 'Daft Punk', 'title': 'Get Lucky'},
        {'artist': 'Radiohead', 'title': 'Creep'},
        {'artist': 'Unknown Artist 12345', 'title': 'Unknown Song 67890'}
    ]

    print("Testing MusicBrainz Genre Enricher\n")

    for song in test_songs:
        print(f"Looking up: {song['artist']} - {song['title']}")
        genre = enricher.lookup_genre(song['artist'], song['title'])
        print(f"  Genre: {genre if genre else 'Not found'}\n")


if __name__ == "__main__":
    test_enricher()
