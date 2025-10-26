from bs4 import BeautifulSoup
import re
import json
from pathlib import Path
from tqdm import tqdm

# constants
VERSION = "King James Version"
INPUT_PATH = Path("bible_text/kjv.html")
OUTPUT_PATH = Path("bible_text/kjv_chunks.jsonl")

# load and parse HTML
soup = BeautifulSoup(INPUT_PATH.read_text(encoding="utf-8"), "html.parser")
elements = soup.find_all(["h2", "p"])

chunks = []
current_book = None

for el in tqdm(elements, desc="Parsing Bible", unit="tag"):
    if el.name == "h2":
        title = el.get_text(strip=True)
        # skip Gutenberg headers or front matter
        if "Project Gutenberg" in title or "License" in title:
            current_book = None
            continue
        current_book = title
    elif el.name == "p" and current_book:
        text = el.get_text(" ", strip=True)
        match = re.match(r"(\d+):(\d+)\s+(.*)", text)
        if match:
            chapter, verse, content = match.groups()
            chunks.append({
                "book": current_book,
                "chapter": chapter,
                "verse": verse,
                "text": content,
                "version": VERSION
            })

# write JSONL output
with OUTPUT_PATH.open("w", encoding="utf-8") as f:
    for c in chunks:
        f.write(json.dumps(c, ensure_ascii=False) + "\n")

print(f"✅ Parsed {len(chunks)} verses → {OUTPUT_PATH}")
