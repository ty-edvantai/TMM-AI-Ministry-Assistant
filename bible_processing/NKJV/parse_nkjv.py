from pathlib import Path
import re
import json
from tqdm import tqdm

RAW_PATH = Path("bible_processing/NKJV/nkjv_raw.txt")
OUT_PATH = Path("bible_processing/NKJV/nkjv_chunks.jsonl")
VERSION = "NKJV"

BOOKS = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth", "1st Samuel", "2nd Samuel",
    "1st Kings", "2nd Kings", "1st Chronicles", "2nd Chronicles",
    "Ezra", "Nehemiah", "Esther", "Job", "Psalms", "Proverbs",
    "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah",
    "Lamentations", "Ezekiel", "Daniel", "Hosea", "Joel", "Amos",
    "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah",
    "Haggai", "Zechariah", "Malachi", "New", "Testament",
    "Matthew", "Mark", "Luke", "John", "Acts", "Romans",
    "1st Corinthians", "2nd Corinthians", "Galatians", "Ephesians",
    "Philippians", "Colossians", "1st Thessalonians", "2nd Thessalonians",
    "1st Timothy", "2nd Timothy", "Titus", "Philemon", "Hebrews",
    "James", "1st Peter", "2nd Peter", "1st John", "2nd John",
    "3rd John", "Jude", "Revelation"
]

BOOK_ALIASES = {title: title for title in BOOKS}
SKIP_TITLES = {"Old", "New", "Testament"}

def is_book_header(line: str) -> str | None:
    s = line.strip()
    if not s or s in SKIP_TITLES:
        return None
    s2 = re.sub(r"\s+\d+$", "", s)
    return BOOK_ALIASES.get(s2)

def clean_text(text: str) -> str:
    text = re.sub(r"[\\/]", "", text)  # Remove stray slashes
    text = re.sub(r"\s+", " ", text)   # Normalize whitespace
    text = re.sub(r"–", "-", text)     # Replace en-dashes with hyphens
    text = re.sub(r"[“”]", '"', text)  # Normalize smart quotes
    text = re.sub(r"[‘’]", "'", text)  # Normalize apostrophes
    return text.strip()

def parse():
    raw = RAW_PATH.read_text(encoding="utf-8")
    lines = [ln.rstrip() for ln in raw.splitlines()]

    chunks = []
    current_book = None
    current_chapter = None
    pending_verse_num = None
    pending_text = []

    def flush():
        nonlocal pending_verse_num, pending_text, current_book, current_chapter
        if current_book and pending_verse_num is not None and pending_text:
            text = " ".join(pending_text).strip()
            text = clean_text(text)
            chunks.append({
                "book": current_book,
                "chapter": str(current_chapter),
                "verse": str(pending_verse_num),
                "text": text,
                "version": VERSION
            })
        pending_verse_num = None
        pending_text = []

    for line in tqdm(lines, desc="Parsing NKJV", unit="line"):
        s = line.strip()

        hdr = is_book_header(s)
        if hdr:
            flush()
            current_book = hdr
            current_chapter = 1  # Always reset to chapter 1 on new book
            continue

        if not current_book or not s:
            continue

        m = re.match(r"^(\d{1,3})\s*(.+)$", s)
        if m:
            verse_num = int(m.group(1))
            verse_text = m.group(2).strip()

            if pending_verse_num is not None:
                flush()

            if current_chapter is None:
                current_chapter = 1

            pending_verse_num = verse_num
            pending_text = [verse_text]
        else:
            if pending_verse_num is not None:
                pending_text.append(s)

    flush()

    OUT_PATH.write_text("\n".join(json.dumps(c, ensure_ascii=False) for c in chunks), encoding="utf-8")
    print(f"✅ Verses parsed: {len(chunks)} → {OUT_PATH}")

if __name__ == "__main__":
    parse()
