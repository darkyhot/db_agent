from dataclasses import dataclass
from typing import Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class DBConfig:
    user_id: str
    host: str
    port: str
    base: str


def get_engine(cfg: DBConfig) -> Engine:
    url = f"postgresql://{cfg.user_id}@{cfg.host}:{cfg.port}/{cfg.base}"
    return create_engine(url)


def validate_sql(engine: Engine, sql_text: str) -> Tuple[bool, Optional[str]]:
    cleaned = sql_text.strip().rstrip(";")
    wrapped = f"SELECT * FROM ({cleaned}) AS t WHERE 1=0"
    try:
        with engine.connect() as conn:
            conn.execute(text(wrapped))
        return True, None
    except Exception as e_wrap:
        try:
            with engine.connect() as conn:
                conn.execute(text(f"EXPLAIN {cleaned}"))
            return True, None
        except Exception as e_exp:
            return False, f"{e_wrap} | {e_exp}"


def run_query(engine: Engine, sql_text: str):
    with engine.connect() as conn:
        return conn.execute(text(sql_text))
