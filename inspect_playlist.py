#!/usr/bin/env python3
"""
Helper script to inspect WDR Cosmo playlist page and identify CSS selectors.

Run this script locally to analyze the HTML structure and get the exact
selectors needed for scraper.py
"""

import requests
from bs4 import BeautifulSoup
from collections import Counter
import sys


def fetch_page(url: str) -> str:
    """Fetch the playlist page."""
    print(f"Fetching {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        print(f"✓ Successfully fetched page ({len(response.text)} bytes)")
        return response.text
    except requests.RequestException as e:
        print(f"✗ Error fetching page: {e}")
        sys.exit(1)


def analyze_structure(html: str):
    """Analyze HTML structure to find song data."""
    soup = BeautifulSoup(html, 'lxml')

    print("\n" + "=" * 80)
    print("ANALYZING PAGE STRUCTURE")
    print("=" * 80)

    # Look for common patterns in song lists
    patterns_to_check = [
        # Common class patterns for song containers
        {'class': 'song'},
        {'class': 'track'},
        {'class': 'playlist-item'},
        {'class': 'playlist-entry'},
        {'class': 'music-item'},
        # Try finding by tag + class combinations
    ]

    # Find all divs, lis, and articles that might contain songs
    potential_containers = []

    for tag in ['div', 'li', 'article', 'section']:
        elements = soup.find_all(tag)
        for elem in elements:
            classes = elem.get('class', [])
            if any('song' in str(c).lower() or
                   'track' in str(c).lower() or
                   'playlist' in str(c).lower() or
                   'music' in str(c).lower() for c in classes):
                potential_containers.append({
                    'tag': tag,
                    'classes': classes,
                    'selector': f"{tag}.{'.'.join(classes)}" if classes else tag
                })

    if potential_containers:
        print("\n📦 Found potential song containers:")
        seen = set()
        for container in potential_containers:
            selector = container['selector']
            if selector not in seen:
                print(f"   - {selector}")
                seen.add(selector)

    # Find all text that looks like time (HH:MM format)
    time_pattern_elements = soup.find_all(string=lambda text: text and ':' in text and len(text.strip()) <= 8)
    time_parents = []
    for elem in time_pattern_elements:
        if elem.parent and elem.strip():
            time_parents.append({
                'tag': elem.parent.name,
                'classes': elem.parent.get('class', []),
                'text': elem.strip(),
                'selector': f"{elem.parent.name}.{'.'.join(elem.parent.get('class', []))}" if elem.parent.get('class') else elem.parent.name
            })

    if time_parents:
        print("\n🕐 Found potential timestamp elements:")
        time_selectors = Counter([t['selector'] for t in time_parents])
        for selector, count in time_selectors.most_common(10):
            print(f"   - {selector} (found {count} times)")

    # Look for <time> tags
    time_tags = soup.find_all('time')
    if time_tags:
        print(f"\n⏰ Found {len(time_tags)} <time> tags:")
        for time_tag in time_tags[:3]:  # Show first 3
            print(f"   - {time_tag}")

    # Try to find repeating structures
    print("\n🔍 Analyzing repeating structures...")

    # Get all elements with classes
    all_elements_with_classes = soup.find_all(class_=True)
    class_counts = Counter()

    for elem in all_elements_with_classes:
        classes = tuple(elem.get('class', []))
        if len(classes) > 0:
            class_counts[classes] += 1

    # Show elements that appear multiple times (likely song entries)
    print("\n📊 Most common class combinations (likely song entries):")
    for classes, count in class_counts.most_common(15):
        if count >= 3:  # At least 3 occurrences
            selector = f".{'.'.join(classes)}"
            print(f"   - {selector} (appears {count} times)")

    return soup


def find_song_data(soup: BeautifulSoup):
    """Try to extract actual song data to verify selectors."""
    print("\n" + "=" * 80)
    print("ATTEMPTING TO EXTRACT SONG DATA")
    print("=" * 80)

    # Get all class combinations that appear multiple times
    all_elements_with_classes = soup.find_all(class_=True)
    class_counts = Counter()

    for elem in all_elements_with_classes:
        classes = tuple(elem.get('class', []))
        if len(classes) > 0:
            class_counts[classes] += 1

    # Try the most common ones as song containers
    for classes, count in class_counts.most_common(10):
        if count < 3:  # Need at least 3 songs
            continue

        selector = '.'.join(classes)
        elements = soup.find_all(class_=list(classes))

        print(f"\n📝 Trying selector: .{selector} ({count} elements)")

        # Analyze first element to understand structure
        if elements:
            first = elements[0]
            print(f"\n   HTML structure of first element:")
            print(f"   {'-' * 70}")
            # Print cleaned up HTML
            print(f"   {str(first)[:500]}...")

            # Try to find text that looks like artist/title
            texts = [t.strip() for t in first.stripped_strings if len(t.strip()) > 2]
            if texts:
                print(f"\n   Extracted text values:")
                for i, text in enumerate(texts[:5], 1):
                    print(f"   {i}. {text}")

            # Look for nested elements
            nested_elements = {}
            for child in first.find_all(True):
                child_classes = ' '.join(child.get('class', []))
                if child_classes:
                    nested_elements[child_classes] = child.get_text(strip=True)[:50]

            if nested_elements:
                print(f"\n   Nested elements with classes:")
                for classes, text in list(nested_elements.items())[:8]:
                    print(f"   .{classes}: '{text}'")

            # Check if there are only a few elements
            if count <= 5:
                print(f"\n   Showing all {count} elements:")
                for i, elem in enumerate(elements, 1):
                    texts = [t.strip() for t in elem.stripped_strings if len(t.strip()) > 2]
                    print(f"   {i}. {' | '.join(texts[:3])}")

    # Also check for table structure
    tables = soup.find_all('table')
    if tables:
        print(f"\n📋 Found {len(tables)} table(s) - might contain playlist data")
        for i, table in enumerate(tables, 1):
            rows = table.find_all('tr')
            print(f"\n   Table {i}: {len(rows)} rows")
            if rows:
                print(f"   First row: {rows[0].get_text(strip=True)[:100]}")


def generate_recommendations(soup: BeautifulSoup):
    """Generate recommendations for scraper.py based on analysis."""
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS FOR scraper.py")
    print("=" * 80)

    print("\nBased on the analysis above, update the selectors in scraper.py:")
    print("\nIn the _parse_playlist method, around line 52-75, update:")
    print("""
    # Example pattern (adjust based on actual structure):
    song_elements = soup.select('.your-song-container-class')

    for element in song_elements:
        artist = element.select_one('.artist-class')
        title = element.select_one('.title-class')
        timestamp = element.select_one('.time-class, time')

        if artist and title:
            song_data = {
                'artist': artist.get_text(strip=True),
                'title': title.get_text(strip=True),
                'timestamp': timestamp.get_text(strip=True) if timestamp else '',
                'date': date.strftime("%Y-%m-%d"),
                'datetime': self._parse_timestamp(
                    timestamp.get_text(strip=True) if timestamp else '',
                    date
                )
            }
            songs.append(song_data)
    """)

    print("\n💡 Tips:")
    print("   - Look for the selector that appears exactly as many times as there are songs")
    print("   - Check the nested elements to find artist, title, and time")
    print("   - Use browser DevTools to verify the selector works")
    print("   - Test with: soup.select('your-selector') in Python")


def save_html_sample(html: str):
    """Save a sample of the HTML for manual inspection."""
    filename = "playlist_sample.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n💾 Saved full HTML to {filename} for manual inspection")


def main():
    url = "https://www1.wdr.de/radio/cosmo/musik/playlist/index.html"

    print("WDR Cosmo Playlist Inspector")
    print("=" * 80)

    html = fetch_page(url)
    save_html_sample(html)

    soup = analyze_structure(html)
    find_song_data(soup)
    generate_recommendations(soup)

    print("\n" + "=" * 80)
    print("INSPECTION COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Review the output above to identify the correct selectors")
    print("2. Open playlist_sample.html in a browser and use DevTools")
    print("3. Update the CSS selectors in scraper.py (lines 52-75)")
    print("4. Test with: python scraper.py")
    print()


if __name__ == "__main__":
    main()
