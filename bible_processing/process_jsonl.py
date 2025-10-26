# ===============================
# 🔧 IMPORTS & SETUP
# ===============================
import os
import json
import openai
from tqdm import tqdm
from supabase import create_client
from dotenv import load_dotenv
from tkinter import Tk, filedialog

# ===============================
# 📌 CONFIGURATION
# ===============================
VECTOR_DIM = 3072
EMBED_MODEL = "text-embedding-3-large"
BUCKET = "materials"
BATCH_SIZE = 100            # Number of verses per embedding batch

# ===============================
# 🧪 TEST MODE CONFIG
# ===============================
TEST_MODE = False           # Set to True to process only a few entries
MAX_TEST_ENTRIES = 5        # Number of entries to process in test mode

# ===============================
# 🔧 ENVIRONMENT VARIABLES
# ===============================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise RuntimeError("Missing one or more environment variables.")

openai.api_key = OPENAI_API_KEY
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===============================
# 📦 UPLOAD FILE TO STORAGE
# ===============================
def upload_file_to_storage(file_path: str):
    file_name = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        data = f.read()
    try:
        res = supabase.storage.from_(BUCKET).upload(file_name, data)
        if hasattr(res, "error") and res.error is not None:
            raise Exception(res.error.message)
        supabase.table("files").insert({
            "file_name": file_name,
            "file_type": ".jsonl",
            "source_path": file_name
        }).execute()
        print(f"📦 Uploaded {file_name} to storage and logged in 'files' table.")
    except Exception as e:
        print(f"⚠️ Storage upload failed for {file_name}: {e}")

# ===============================
# 🔢 BATCH EMBEDDING
# ===============================
def embed_batch(texts: list[str]) -> list[list[float]]:
    try:
        response = openai.embeddings.create(input=texts, model=EMBED_MODEL)
        embeddings = [item.embedding for item in response.data]
        for i, emb in enumerate(embeddings):
            if len(emb) != VECTOR_DIM:
                print(f"⚠️ Embedding {i} has dimension {len(emb)} (expected {VECTOR_DIM})")
        return embeddings
    except Exception as e:
        print(f"❌ Embedding batch failed: {e}")
        return [None] * len(texts)

# ===============================
# 🚀 BATCH UPLOAD TO SUPABASE
# ===============================
def upload_batch(chunks: list[dict], embeddings: list[list[float]], file_path: str):
    payload = []
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        if not embedding:
            print(f"⚠️ Skipping chunk {idx} due to missing embedding")
            continue
        metadata = {
            "source_file": os.path.basename(file_path),
            "chunk_index": idx,
            "book": chunk.get("book"),
            "chapter": chunk.get("chapter"),
            "verse": chunk.get("verse"),
            "version": chunk.get("version"),
            "file_type": ".jsonl"
        }
        payload.append({
            "content": chunk["text"],
            "embedding": embedding,
            "metadata": metadata
        })
    if payload:
        try:
            supabase.table("documents").insert(payload).execute()
        except Exception as e:
            print(f"❌ Supabase batch insert error: {e}")

# ===============================
# 🧠 PROCESS JSONL FILE
# ===============================
def process_jsonl_file(file_path: str):
    print(f"\n📘 Processing JSONL: {file_path}")
    upload_file_to_storage(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if TEST_MODE:
        lines = lines[:MAX_TEST_ENTRIES]
        print(f"🧪 Test mode ON — processing first {MAX_TEST_ENTRIES} entries")

    chunks = [json.loads(line) for line in lines if "text" in json.loads(line)]
    print(f"🧩 Total chunks: {len(chunks)}")

    with tqdm(total=len(chunks), desc="Embedding + Uploading", leave=False) as pbar:
        for i in range(0, len(chunks), BATCH_SIZE):
            batch = chunks[i:i + BATCH_SIZE]
            texts = [c["text"].strip() for c in batch]
            embeddings = embed_batch(texts)
            upload_batch(batch, embeddings, file_path)
            pbar.update(len(batch))

    print(f"\n✅ Completed {file_path}")

# ===============================
# 📂 GUI FILE PICKER
# ===============================
def select_jsonl_file() -> str:
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Select a JSONL file",
        filetypes=[("JSONL files", "*.jsonl")]
    )
    return file_path

# ===============================
# ▶️ ENTRY POINT
# ===============================
if __name__ == "__main__":
    file_path = select_jsonl_file()
    if not file_path:
        print("❌ No file selected. Exiting.")
    else:
        process_jsonl_file(file_path)