import sqlalchemy as sa
from sqlalchemy import create_engine
import os

db_url = os.environ.get("OPENALEX_DATABASE_URL")
if not db_url:
    print("Database URL not set")
    exit(1)

engine = create_engine(db_url)
with engine.connect() as conn:
    # Set search_path
    conn.execute(sa.text("SET search_path TO raw, public;"))
    
    query = """
    EXPLAIN ANALYZE
    SELECT id
    FROM openalex_work_versions
    WHERE payload->>'publication_date' > '2026-01-01'
    AND payload->>'type' = 'article';
    """
    
    result = conn.execute(sa.text(query))
    for row in result:
        print(row[0])
