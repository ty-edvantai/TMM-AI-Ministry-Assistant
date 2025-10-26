from pathlib import Path
import fitz  # PyMuPDF

# paths
input_pdf = Path("bible_processing/NKJV/nkjv.pdf")
output_txt = Path("bible_processing/NKJV/nkjv_raw.txt")

# extract all text
text = []
with fitz.open(input_pdf) as doc:
    for page in doc:
        page_text = page.get_text("text")
        text.append(page_text)

# join pages with double newlines
clean_text = "\n\n".join(text)

# save
output_txt.write_text(clean_text, encoding="utf-8")

print(f"✅ Extracted {len(text)} pages from {input_pdf.name}")
print(f"→ Saved to {output_txt}")