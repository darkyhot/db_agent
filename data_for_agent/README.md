# db_agent data_for_agent

Папка `data_for_agent` содержит CSV-файлы с метаданными БД.

Ожидаемые файлы:
- `tables_list.csv` — таблица и описание
  - колонки: `schema_name`, `table_name`, `description`
- `attr_list.csv` — таблица и атрибуты
  - колонки: `schema_name`, `table_name`, `column_name`, `dType`, `is_not_null`, `description`, `is_primary_key`, `not_null_perc`, `unique_perc`

Примечания:
- Файлы могут быть крупными (тысячи строк). Это поддерживается.
- Код читает CSV как строки (dtype=str).
