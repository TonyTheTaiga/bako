from functools import lru_cache
from typing import List
import struct
from pathlib import Path

import sqlite3
import sqlite_vec
from openai import OpenAI


@lru_cache(maxsize=1)
def create_oai_client():
    return OpenAI()

def get_embedding(text, model="text-embedding-3-small"):
    text = text.replace("\n", " ")
    return create_oai_client().embeddings.create(input = [text], model=model, dimensions=512).data[0].embedding

db = sqlite3.connect("/Users/taigaishida/workspace/bako/bako.db")
db.enable_load_extension(True)
sqlite_vec.load(db)
db.enable_load_extension(False)

vec_version, = db.execute("select vec_version()").fetchone()
print(f"vec_version={vec_version}")

db.execute("DROP TABLE IF EXISTS vec_items")
db.execute("CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[512] DISTANCE_METRIC=cosine, file_id text)")
db.execute(
    """
    INSERT INTO vec_items(embedding, file_id)
    SELECT vec_f32(embedding), file_id
    FROM embeddings;
    """
    )

query = "my name is taiga..."
embedding = get_embedding(query)

def serialize_f32(vector: List[float]) -> bytes:
    """serializes a list of floats into a compact "raw bytes" format"""
    return struct.pack("%sf" % len(vector), *vector)

rows = db.execute(
    """
    WITH results AS (
      SELECT
        file_id,
        distance
      FROM vec_items
      WHERE embedding MATCH ?
      ORDER BY distance
      LIMIT 1
    )
    SELECT files.path, results.distance from results
    left join files on results.file_id = files.id
    """,
    [serialize_f32(embedding)],
).fetchall()

print(rows)

texts = []
for row in rows:
    file_path, distance = row
    texts.append(Path(file_path).read_text(encoding='utf-8').strip())

print('\n---\n'.join(texts))