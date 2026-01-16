"""
Genre enrichment using Last.fm API.

Last.fm has much better tag coverage than MusicBrainz due to user-driven scrobbling data.
Rate limit: 5 requests per second.
"""

import os
import re
import requests
import time
from typing import Optional, List
from dotenv import load_dotenv
from rapidfuzz import fuzz

# Load environment variables from .env file
load_dotenv()


class LastFmGenreEnricher:
    BASE_URL = "https://ws.audioscrobbler.com/2.0/"
    RATE_LIMIT_DELAY = 0.2  # 5 requests per second allowed
    MIN_FUZZY_SCORE = 50.0

    # Patterns to strip from titles for better matching
    TITLE_STRIP_PATTERNS = [
        r'\s*\(feat\.?\s+[^)]+\)',       # (feat. Artist)
        r'\s*\(ft\.?\s+[^)]+\)',          # (ft. Artist)
        r'\s*\(featuring\s+[^)]+\)',      # (featuring Artist)
        r'\s*\(with\s+[^)]+\)',           # (with Artist)
        r'\s*\(remix\)',                   # (remix)
        r'\s*\([^)]*remix[^)]*\)',         # (Something Remix)
        r'\s*\(radio\s*edit\)',            # (radio edit)
        r'\s*\(radio\s*version\)',         # (radio version)
        r'\s*\(edit\)',                    # (edit)
        r'\s*\(original\s*mix\)',          # (original mix)
        r'\s*\(extended\s*mix\)',          # (extended mix)
        r'\s*\(club\s*mix\)',              # (club mix)
        r'\s*\(acoustic\)',                # (acoustic)
        r'\s*\(live\)',                    # (live)
        r'\s*\(remaster(ed)?\)',           # (remaster) or (remastered)
        r'\s*\([0-9]{4}\s*remaster\)',     # (2021 remaster)
        r'\s*-\s*remix$',                  # - Remix at end
        r'\s*-\s*radio\s*edit$',           # - Radio Edit at end
    ]

    def __init__(self, api_key: str = None, verbose: bool = False):
        """
        Initialize the Last.fm genre enricher.

        Args:
            api_key: Last.fm API key. If not provided, reads from LASTFM_API_KEY env var.
            verbose: Enable detailed logging
        """
        self.api_key = api_key or os.environ.get('LASTFM_API_KEY')
        if not self.api_key:
            raise ValueError(
                "Last.fm API key required. Set LASTFM_API_KEY environment variable "
                "or pass api_key parameter. Get a free key at: https://www.last.fm/api/account/create"
            )

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WDRCosmoAnalyzer/0.1.0'
        })
        self.last_request_time = 0
        self.verbose = verbose

    def _rate_limit(self):
        """Ensure we respect Last.fm rate limit of 5 requests/second."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def _clean_title(self, title: str) -> str:
        """Remove common suffixes from song titles for better matching."""
        cleaned = title
        for pattern in self.TITLE_STRIP_PATTERNS:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    def _extract_primary_artist(self, artist: str) -> str:
        """Extract primary artist name from multi-artist string."""
        # Split on common separators and take first
        for sep in [' feat.', ' feat ', ' ft.', ' ft ', ' & ', ' x ', ' vs ', ' vs. ', ', ']:
            if sep in artist.lower():
                idx = artist.lower().find(sep)
                return artist[:idx].strip()
        return artist.strip()

    def _get_track_tags(self, artist: str, title: str) -> Optional[List[str]]:
        """
        Get top tags for a track from Last.fm.

        Returns list of tag names or None if not found.
        """
        self._rate_limit()

        try:
            params = {
                'method': 'track.getTopTags',
                'artist': artist,
                'track': title,
                'api_key': self.api_key,
                'format': 'json'
            }

            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Check for errors
            if 'error' in data:
                if self.verbose:
                    print(f"    -> Last.fm error: {data.get('message', 'Unknown error')}")
                return None

            tags = data.get('toptags', {}).get('tag', [])

            if not tags:
                return None

            # Handle single tag (Last.fm returns dict instead of list)
            if isinstance(tags, dict):
                tags = [tags]

            # Filter out low-count tags and return names
            return [tag['name'] for tag in tags if int(tag.get('count', 0)) > 0][:5]

        except requests.RequestException as e:
            if self.verbose:
                print(f"    -> Request error: {e}")
            return None
        except (KeyError, ValueError) as e:
            if self.verbose:
                print(f"    -> Parse error: {e}")
            return None

    def _get_artist_tags(self, artist: str) -> Optional[List[str]]:
        """
        Get top tags for an artist from Last.fm.

        Fallback when track tags are not available.
        """
        self._rate_limit()

        try:
            params = {
                'method': 'artist.getTopTags',
                'artist': artist,
                'api_key': self.api_key,
                'format': 'json'
            }

            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if 'error' in data:
                if self.verbose:
                    print(f"    -> Last.fm artist error: {data.get('message', 'Unknown error')}")
                return None

            tags = data.get('toptags', {}).get('tag', [])

            if not tags:
                return None

            if isinstance(tags, dict):
                tags = [tags]

            return [tag['name'] for tag in tags if int(tag.get('count', 0)) > 0][:5]

        except requests.RequestException as e:
            if self.verbose:
                print(f"    -> Artist request error: {e}")
            return None
        except (KeyError, ValueError) as e:
            if self.verbose:
                print(f"    -> Artist parse error: {e}")
            return None

    def _format_tags(self, tags: List[str]) -> str:
        """Format tag list as comma-separated string."""
        return ', '.join(tags[:3])

    def lookup_genre(self, artist: str, title: str) -> Optional[str]:
        """
        Look up genre/tags for a song from Last.fm.

        Strategy:
        1. Try exact artist + title
        2. Try with cleaned title (strip remix, edit, etc.)
        3. Try primary artist only + title
        4. Try primary artist only + cleaned title
        5. Fallback to artist tags

        Args:
            artist: Artist name
            title: Song title

        Returns:
            Genre/tags string (comma-separated) or None if not found
        """
        if self.verbose:
            print(f"  Looking up: {artist} - {title}")

        # Strategy 1: Exact match
        tags = self._get_track_tags(artist, title)
        if tags:
            if self.verbose:
                print(f"    -> Found track tags (exact)")
            return self._format_tags(tags)

        # Strategy 2: Cleaned title
        cleaned_title = self._clean_title(title)
        if cleaned_title != title:
            if self.verbose:
                print(f"    -> Trying cleaned title: '{cleaned_title}'")
            tags = self._get_track_tags(artist, cleaned_title)
            if tags:
                if self.verbose:
                    print(f"    -> Found track tags (cleaned title)")
                return self._format_tags(tags)

        # Strategy 3: Primary artist + original title
        primary_artist = self._extract_primary_artist(artist)
        if primary_artist != artist:
            if self.verbose:
                print(f"    -> Trying primary artist: '{primary_artist}'")
            tags = self._get_track_tags(primary_artist, title)
            if tags:
                if self.verbose:
                    print(f"    -> Found track tags (primary artist)")
                return self._format_tags(tags)

            # Strategy 4: Primary artist + cleaned title
            if cleaned_title != title:
                tags = self._get_track_tags(primary_artist, cleaned_title)
                if tags:
                    if self.verbose:
                        print(f"    -> Found track tags (primary artist + cleaned title)")
                    return self._format_tags(tags)

        # Strategy 5: Artist tags fallback
        if self.verbose:
            print(f"    -> Trying artist tags fallback")

        tags = self._get_artist_tags(primary_artist if primary_artist != artist else artist)
        if tags:
            if self.verbose:
                print(f"    -> Found artist tags")
            return self._format_tags(tags)

        if self.verbose:
            print(f"    -> FAILED: No tags found")

        return None

    def enrich_songs(
        self,
        songs: List[dict],
        on_progress=None,
        not_found_log: str = None
    ) -> dict:
        """
        Enrich multiple songs with genre information.

        Args:
            songs: List of dicts with 'artist' and 'title' keys
            on_progress: Optional callback function(current, total, artist, title, genre)
            not_found_log: Optional file path to log not-found songs

        Returns:
            Dictionary with statistics
        """
        stats = {
            'total': len(songs),
            'found': 0,
            'not_found': 0,
            'not_found_songs': [],
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
                    stats['not_found_songs'].append({'artist': artist, 'title': title})
                    if on_progress:
                        on_progress(i, len(songs), artist, title, None)

            except Exception as e:
                stats['errors'] += 1
                print(f"  Unexpected error for {artist} - {title}: {e}")

        # Write not-found songs to log file
        if not_found_log and stats['not_found_songs']:
            with open(not_found_log, 'w', encoding='utf-8') as f:
                f.write("# Songs not found in Last.fm\n")
                f.write(f"# Total: {stats['not_found']}\n\n")
                for song in stats['not_found_songs']:
                    f.write(f"{song['artist']} - {song['title']}\n")
            print(f"\nNot-found songs logged to: {not_found_log}")

        return stats


def test_enricher():
    """Test the Last.fm genre enricher."""
    api_key = os.environ.get('LASTFM_API_KEY')
    if not api_key:
        print("Set LASTFM_API_KEY environment variable to test")
        print("Get a free key at: https://www.last.fm/api/account/create")
        return

    enricher = LastFmGenreEnricher(api_key=api_key, verbose=True)

    test_songs = [
        # Simple cases
        {'artist': 'Daft Punk', 'title': 'Get Lucky'},
        {'artist': 'Radiohead', 'title': 'Creep'},
        # Multi-artist cases
        {'artist': 'Dua Lipa feat. DaBaby', 'title': 'Levitating'},
        {'artist': 'David Guetta & Sia', 'title': 'Titanium'},
        # The problematic ones from MusicBrainz
        {'artist': 'Aya Nakamura', 'title': 'Pookie'},
        {'artist': 'Aya Nakamura', 'title': 'Jolie nana'},
        {'artist': 'Aya Nakamura & Kali Uchis', 'title': 'Baby boy'},
        # Title with suffix
        {'artist': 'The Weeknd', 'title': 'Blinding Lights (Radio Edit)'},
        # Unknown (should return None)
        {'artist': 'Unknown Artist 12345', 'title': 'Unknown Song 67890'}
    ]

    print("Testing Last.fm Genre Enricher")
    print("=" * 60)

    found = 0
    for song in test_songs:
        print(f"\n{song['artist']} - {song['title']}")
        genre = enricher.lookup_genre(song['artist'], song['title'])
        if genre:
            found += 1
            print(f"  => {genre}")
        else:
            print(f"  => NOT FOUND")

    print("\n" + "=" * 60)
    print(f"Results: {found}/{len(test_songs)} songs found")


if __name__ == "__main__":
    test_enricher()
