from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

DATA_DIR = Path("data_for_agent")


@dataclass
class TableInfo:
    schema_name: str
    table_name: str
    description: str


@dataclass
class ColumnInfo:
    schema_name: str
    table_name: str
    column_name: str
    dtype: str
    is_not_null: str
    description: str
    is_primary_key: str
    not_null_perc: str
    unique_perc: str


class MetadataStore:
    def __init__(self, data_dir: Path = DATA_DIR) -> None:
        self.data_dir = data_dir
        self.tables_df: Optional[pd.DataFrame] = None
        self.attrs_df: Optional[pd.DataFrame] = None
        self._table_index: Dict[str, TableInfo] = {}
        self._attr_index: Dict[str, List[ColumnInfo]] = {}

    def load(self) -> None:
        tables_path = self.data_dir / "tables_list.csv"
        attrs_path = self.data_dir / "attr_list.csv"
        if tables_path.exists():
            self.tables_df = pd.read_csv(tables_path, dtype=str, low_memory=False)
        if attrs_path.exists():
            self.attrs_df = pd.read_csv(attrs_path, dtype=str, low_memory=False)
        self._build_indexes()

    def _build_indexes(self) -> None:
        self._table_index = {}
        self._attr_index = {}
        if self.tables_df is not None:
            for _, row in self.tables_df.iterrows():
                key = f"{row.get('schema_name','')}.{row.get('table_name','')}"
                self._table_index[key] = TableInfo(
                    schema_name=str(row.get("schema_name", "")),
                    table_name=str(row.get("table_name", "")),
                    description=str(row.get("description", "")),
                )
        if self.attrs_df is not None:
            for _, row in self.attrs_df.iterrows():
                key = f"{row.get('schema_name','')}.{row.get('table_name','')}"
                info = ColumnInfo(
                    schema_name=str(row.get("schema_name", "")),
                    table_name=str(row.get("table_name", "")),
                    column_name=str(row.get("column_name", "")),
                    dtype=str(row.get("dType", "")),
                    is_not_null=str(row.get("is_not_null", "")),
                    description=str(row.get("description", "")),
                    is_primary_key=str(row.get("is_primary_key", "")),
                    not_null_perc=str(row.get("not_null_perc", "")),
                    unique_perc=str(row.get("unique_perc", "")),
                )
                self._attr_index.setdefault(key, []).append(info)

    def list_tables(self) -> List[TableInfo]:
        return list(self._table_index.values())

    def find_tables(self, keyword: str, limit: int = 20) -> List[TableInfo]:
        if self.tables_df is None:
            return []
        kw = keyword.lower().strip()
        if not kw:
            return []
        df = self.tables_df
        mask = (
            df["table_name"].fillna("").str.lower().str.contains(kw)
            | df["description"].fillna("").str.lower().str.contains(kw)
        )
        rows = df[mask].head(limit)
        return [
            TableInfo(
                schema_name=str(r.get("schema_name", "")),
                table_name=str(r.get("table_name", "")),
                description=str(r.get("description", "")),
            )
            for _, r in rows.iterrows()
        ]

    def get_table_details(self, schema: str, table: str) -> Dict[str, object]:
        key = f"{schema}.{table}"
        table_info = self._table_index.get(key)
        attrs = self._attr_index.get(key, [])
        return {
            "table": table_info,
            "columns": attrs,
        }
