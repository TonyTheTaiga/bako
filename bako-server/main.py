from typing import List
from functools import lru_cache
import struct
from pathlib import Path

import sqlite3
import sqlite_vec
from mcp.server.fastmcp import FastMCP
from openai import OpenAI


@lru_cache(maxsize=1)
def get_db_connection():
    """Create and return a database connection."""
    conn = sqlite3.connect("/Users/taigaishida/workspace/bako/bako.db")
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn


def build_index():
    db = get_db_connection()    
    db.execute("DROP TABLE IF EXISTS vec_items")
    db.execute("CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[512] distance_metric=cosine, file_id text)")
    db.execute(
        """
        INSERT INTO vec_items(embedding, file_id)
        SELECT vec_f32(embedding), file_id
        FROM embeddings;
        """
    )

@lru_cache(maxsize=1)
def create_oai_client():
    return OpenAI()

def get_embedding(text, model="text-embedding-3-small"):
    text = text.replace("\n", " ")
    return create_oai_client().embeddings.create(input = [text], model=model, dimensions=512).data[0].embedding

def serialize_f32(vector: List[float]) -> bytes:
    """serializes a list of floats into a compact "raw bytes" format"""
    return struct.pack("%sf" % len(vector), *vector)


build_index()

USER_AGENT = "bako-server/1.0"
mcp = FastMCP("bako-server")

@mcp.tool()
def who(name: str) -> str:
    """
    Get information about a person based on their name.

    Args:
        name: The name to query against the database for similar content.
    """

    try:
        embedding = get_embedding(name)
        db = get_db_connection()
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

        if not rows:
            return "No information found."

        texts = []
        for row in rows:
            file_path, distance = row
            try:
                texts.append(Path(file_path).read_text(encoding='utf-8').strip())
            except Exception as e:
                return f"Error reading file: {file_path}: {str(e)}"


        return '\\n---\\n'.join(texts)
    except Exception as e:
        return f"Error processing request: {str(e)}"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')