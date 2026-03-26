"""
Spotify integration for genre enrichment and playlist export.

Uses spotipy for OAuth authentication and Spotify API access.
"""

import os
import re
from typing import Optional, List, Dict
from difflib import SequenceMatcher

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()


class SpotifyClient:
    SCOPES = "playlist-modify-public playlist-modify-private"

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

    def __init__(self, verbose: bool = False):
        """
        Initialize Spotify client with OAuth.

        First run will open browser for authentication.
        """
        client_id = os.environ.get('SPOTIFY_CLIENT_ID')
        client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
        redirect_uri = os.environ.get('SPOTIFY_REDIRECT_URI', 'http://127.0.0.1:8888/callback')

        if not client_id or not client_secret:
            raise ValueError(
                "Spotify credentials required. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET "
                "environment variables. Create an app at: https://developer.spotify.com/dashboard"
            )

        if verbose:
            print(f"Spotify OAuth config:")
            print(f"  Client ID: {client_id[:8]}...{client_id[-4:]}")
            print(f"  Redirect URI: {redirect_uri}")
            print(f"  Scopes: {self.SCOPES}")
            print(f"  Cache: .spotify_cache")

        auth_manager = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=self.SCOPES,
            cache_path=".spotify_cache",
            open_browser=True
        )

        # Print auth URL for debugging
        if verbose:
            auth_url = auth_manager.get_authorize_url()
            print(f"  Auth URL: {auth_url}")

        # Check for cached token first
        token_info = auth_manager.cache_handler.get_cached_token()
        if token_info and not auth_manager.is_token_expired(token_info):
            if verbose:
                print("  Using cached token")
        else:
            if token_info:
                if verbose:
                    print("  Cached token expired, refreshing...")
                try:
                    token_info = auth_manager.refresh_access_token(token_info['refresh_token'])
                    if verbose:
                        print("  Token refreshed successfully")
                except Exception as e:
                    print(f"  Token refresh failed: {e}")
                    print("  Will re-authenticate via browser...")
                    token_info = None
            else:
                print("  No cached token found, opening browser for authentication...")
                print(f"  Make sure '{redirect_uri}' is registered in your Spotify app dashboard")

        try:
            self.sp = spotipy.Spotify(auth_manager=auth_manager)
            # Test the connection
            user = self.sp.current_user()
            if verbose:
                print(f"  Authenticated as: {user['display_name']} ({user['id']})")
        except spotipy.SpotifyOauthError as e:
            raise ValueError(
                f"Spotify OAuth error: {e}\n\n"
                f"Troubleshooting:\n"
                f"  1. Check that Client ID and Secret are correct\n"
                f"  2. Verify redirect URI '{redirect_uri}' matches EXACTLY in your Spotify dashboard\n"
                f"  3. Delete .spotify_cache and try again\n"
                f"  4. Make sure your Spotify app is not in 'Development mode' restrictions"
            )
        except spotipy.SpotifyException as e:
            raise ValueError(f"Spotify API error: {e}")
        except Exception as e:
            raise ValueError(
                f"Failed to connect to Spotify: {e}\n\n"
                f"Try deleting .spotify_cache and re-authenticating."
            )

        self.verbose = verbose
        self._artist_genre_cache: Dict[str, List[str]] = {}
        self._user_id: Optional[str] = None

    def _get_user_id(self) -> str:
        """Get current user's Spotify ID (cached)."""
        if not self._user_id:
            self._user_id = self.sp.current_user()['id']
        return self._user_id

    def _clean_title(self, title: str) -> str:
        """Remove common suffixes from song titles for better matching."""
        cleaned = title
        for pattern in self.TITLE_STRIP_PATTERNS:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    def _extract_primary_artist(self, artist: str) -> str:
        """Extract primary artist name from multi-artist string."""
        for sep in [' feat.', ' feat ', ' ft.', ' ft ', ' & ', ' x ', ' vs ', ' vs. ', ', ']:
            if sep in artist.lower():
                idx = artist.lower().find(sep)
                return artist[:idx].strip()
        return artist.strip()

    def _similarity(self, a: str, b: str) -> float:
        """Calculate string similarity ratio."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()

    def search_track(self, artist: str, title: str) -> Optional[Dict]:
        """
        Search Spotify for a track.

        Returns:
            Dict with {track_id, artist_id, name, artists} or None if not found
        """
        # Try multiple search strategies
        cleaned_title = self._clean_title(title)
        primary_artist = self._extract_primary_artist(artist)

        queries = [
            f"artist:{artist} track:{title}",
            f"artist:{artist} track:{cleaned_title}" if cleaned_title != title else None,
            f"artist:{primary_artist} track:{title}" if primary_artist != artist else None,
            f"artist:{primary_artist} track:{cleaned_title}" if primary_artist != artist and cleaned_title != title else None,
            f"{artist} {title}",  # Fallback: simple search
        ]

        for query in filter(None, queries):
            try:
                results = self.sp.search(q=query, type='track', limit=5)
                tracks = results.get('tracks', {}).get('items', [])

                if not tracks:
                    continue

                # Find best match using fuzzy matching
                best_match = None
                best_score = 0.0

                for track in tracks:
                    track_name = track['name']
                    track_artists = [a['name'] for a in track['artists']]

                    # Calculate similarity scores
                    title_score = max(
                        self._similarity(title, track_name),
                        self._similarity(cleaned_title, track_name)
                    )
                    artist_score = max(
                        self._similarity(artist, a) for a in track_artists
                    )

                    # Combined score
                    score = (title_score + artist_score) / 2

                    if score > best_score and score > 0.5:  # Minimum threshold
                        best_score = score
                        best_match = track

                if best_match:
                    if self.verbose:
                        print(f"    -> Found: {best_match['artists'][0]['name']} - {best_match['name']} (score: {best_score:.2f})")

                    return {
                        'track_id': best_match['id'],
                        'artist_id': best_match['artists'][0]['id'],
                        'name': best_match['name'],
                        'artists': [a['name'] for a in best_match['artists']]
                    }

            except Exception as e:
                if self.verbose:
                    print(f"    -> Search error: {e}")
                continue

        return None

    def create_playlist(self, name: str, track_ids: List[str], description: str = "") -> str:
        """
        Create a new Spotify playlist with the given tracks.

        Args:
            name: Playlist name
            track_ids: List of Spotify track IDs
            description: Optional playlist description

        Returns:
            Playlist URL
        """
        user_id = self._get_user_id()

        # Create playlist
        playlist = self.sp.user_playlist_create(
            user=user_id,
            name=name,
            public=True,
            description=description
        )

        playlist_id = playlist['id']

        # Add tracks in batches of 100 (Spotify limit)
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            # Convert track IDs to URIs
            uris = [f"spotify:track:{tid}" for tid in batch]
            self.sp.playlist_add_items(playlist_id, uris)

        return playlist['external_urls']['spotify']

    def update_playlist(self, playlist_id: str, track_ids: List[str]):
        """
        Replace all tracks in an existing playlist.

        Args:
            playlist_id: Spotify playlist ID
            track_ids: List of Spotify track IDs
        """
        # Clear existing tracks and add new ones
        uris = [f"spotify:track:{tid}" for tid in track_ids[:100]]
        self.sp.playlist_replace_items(playlist_id, uris)

        # Add remaining tracks in batches
        for i in range(100, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            uris = [f"spotify:track:{tid}" for tid in batch]
            self.sp.playlist_add_items(playlist_id, uris)
