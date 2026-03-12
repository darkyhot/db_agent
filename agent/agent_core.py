import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from .config import ConfigStore, DBConfig as StoredDBConfig
from .db import DBConfig, get_engine, validate_sql
from .fs_ops import FileSandbox
from .llm_client import LLMClient
from .memory import MemoryStore
from .metadata import MetadataStore


@dataclass
class AgentSettings:
    max_iters: int = 5  # увеличил для множества действий
    max_llm_calls: int = 10  # увеличил
    memory_window: int = 15
    summarize_every: int = 20


class Agent:
    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.config_store = ConfigStore()
        self.memory = MemoryStore()
        self.metadata = MetadataStore()
        self.metadata.load()
        self.fs = FileSandbox(workdir)
        self.llm = LLMClient()
        self.settings = AgentSettings()

    def status(self) -> str:
        cfg = self.config_store.load()
        msg_count = self.memory.count_messages()
        tables_loaded = len(self.metadata.list_tables())
        return (
            f"DB config complete: {cfg.is_complete()}\n"
            f"Messages stored: {msg_count}\n"
            f"Tables metadata loaded: {tables_loaded}"
        )

    def reset(self) -> None:
        self.memory.reset()

    def _maybe_summarize(self) -> None:
        count = self.memory.count_messages()
        if count > 0 and count % self.settings.summarize_every == 0:
            summary = self.memory.get_summary()
            recent = self.memory.get_recent(self.settings.memory_window)
            convo = "\n".join([f"{m.role}: {m.content}" for m in recent])
            prompt = (
                "Ты ассистент. Обнови краткую выжимку диалога.\n"
                f"Текущая выжимка: {summary}\n"
                f"Новые сообщения:\n{convo}\n"
                "Верни обновленную выжимку 5-10 предложений."
            )
            resp = self.llm.invoke(prompt)
            text = getattr(resp, "content", str(resp))
            self.memory.set_summary(text)

    def _get_table_samples(self) -> str:
        """Получает примеры данных из таблиц для контекста."""
        cfg = self.config_store.load()
        if not cfg.is_complete():
            return ""
        try:
            engine = get_engine(DBConfig(cfg.user_id, cfg.host, cfg.port, cfg.base))
            samples = []
            for table_info in list(self.metadata._table_index.values())[:3]:  # max 3 таблицы
                key = f"{table_info.schema_name}.{table_info.table_name}"
                try:
                    with engine.connect() as conn:
                        result = conn.execute(text(f'SELECT * FROM {key} LIMIT 3'))
                        rows = result.fetchall()
                        if rows:
                            cols = result.keys()
                            lines = [";".join(cols)]
                            for row in rows:
                                lines.append(";".join(str(c)[:50] for c in row))  # обрезаем длинные строки
                            samples.append(f"{key} (первые 3 строки):\n" + "\n".join(lines))
                except Exception:
                    continue
            return "\n\n".join(samples) if samples else ""
        except Exception:
            return ""

    def _build_context(self, extra: str = "") -> str:
        summary = self.memory.get_summary()
        recent = self.memory.get_recent(self.settings.memory_window)
        convo = "\n".join([f"{m.role}: {m.content}" for m in recent])
        meta_hint = ""
        samples = ""
        if self.metadata.tables_df is not None:
            schema_summary = self.metadata.get_schema_summary()
            meta_hint = f"Доступны таблицы и атрибуты из CSV метаданных.\nСхема БД:\n{schema_summary}"
            samples = self._get_table_samples()
            if samples:
                meta_hint += f"\n\nПримеры реальных данных:\n{samples}"
        return (
            f"Краткая память: {summary}\n"
            f"Последние сообщения:\n{convo}\n"
            f"Метаданные: {meta_hint}\n"
            f"Ошибки и замечания: {extra}\n"
        )

    def _json_from_response(self, text: str) -> Optional[Dict[str, Any]]:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except Exception:
            return None

    def _plan_prompt(self, user_text: str, context: str) -> str:
        return (
            "Ты агент по работе с БД и файлами. Сначала составь план.\n"
            f"Контекст:\n{context}\n"
            f"Запрос пользователя: {user_text}\n"
            "Верни план в виде нумерованного списка (3-6 пунктов)."
        )

    def _action_prompt(self, user_text: str, plan: str, context: str, executed_steps: List[str]) -> str:
        steps_done = "\n".join([f"{i+1}. {s}" for i, s in enumerate(executed_steps)]) if executed_steps else "Нет"
        return (
            "Ты агент по работе с БД, SQL и файловой системой.\n"
            "Сформируй ОДНО действие в JSON. Если нужно несколько действий - делай по одному за раз.\n"
            "Формат JSON:\n"
            "{\n"
            '  "type": "sql|fs|answer|model_design|question|done",\n'
            '  "content": "ответ/пояснение",\n'
            '  "sql": "SQL код",\n'
            '  "run_sql": true/false,\n'
            '  "show_sql": true/false,\n'
            '  "output_file": "путь/к/file.csv",\n'
            '  "fs_ops": [{"op": "...", "path": "..."}]\n'
            "}\n\n"
            "ПРАВИЛА:\n"
            "1. ОБЫЧНЫЙ ВОПРОС: type='sql', run_sql=true, show_sql=false\n"
            "2. ПРОСЯТ ПОКАЗАТЬ SQL: show_sql=true\n"
            "3. ПРОСЯТ ФАЙЛ: output_file='путь/к/файлу.csv'\n"
            "4. type='done' - когда все шаги плана выполнены\n\n"
            "АНАЛИЗ ДАННЫХ:\n"
            "- Используй описания таблиц [в скобках] и колонок\n"
            "- Примеры данных - реальные значения\n\n"
            "ПРАВИЛА ДЛЯ SQL:\n"
            "- Проверяй существование полей в схеме\n"
            "- Не преобразовывай даты если поле уже в нужном формате\n"
            "- Не делай JOIN если данные уже нормализованы\n"
            "- **ВАЖНО: При JOIN справочников используй подзапрос с DISTINCT:**\n"
            "  SELECT ... FROM table t\n"
            "  JOIN (SELECT DISTINCT gosb_id, gosb_name FROM dim_gosb) d ON t.gosb_id = d.gosb_id\n"
            "  Это предотвращает дублирование строк при множественных совпадениях\n"
            "- Для агрегации по месяцу: GROUP BY поле_месяц (без DATE_TRUNC)\n"
            "- При неуверенности смотри описание поля в схеме\n\n"}
        # ... truncated for brevity
            f"Контекст:\n{context}\n"
            f"План:\n{plan}\n"
            f"Уже выполнено:\n{steps_done}\n"
            f"Запрос пользователя: {user_text}\n"
            "Следующий шаг (type='done' если всё готово):"
        )

    def _fix_sql_prompt(self, user_text: str, bad_sql: str, error: str, context: str) -> str:
        return (
            "Исправь SQL запрос.\n"
            f"Ошибка: {error}\n"
            f"Плохой SQL:\n{bad_sql}\n"
            f"Контекст:\n{context}\n"
            f"Запрос пользователя: {user_text}\n"
            "Верни только исправленный SQL без пояснений."
        )

    def _execute_fs_ops(self, ops: List[Dict[str, Any]]) -> List[str]:
        results = []
        for op in ops:
            action = op.get("op")
            path = op.get("path", "")
            if action == "read":
                results.append(self.fs.read_text(path))
            elif action == "write":
                self.fs.write_text(path, op.get("content", ""))
                results.append(f"written: {path}")
            elif action == "mkdir":
                self.fs.mkdir(path)
                results.append(f"mkdir: {path}")
            elif action == "rm":
                self.fs.rm(path)
                results.append(f"rm: {path}")
            elif action == "ls":
                results.append("\n".join(self.fs.ls(path)))
        return results

    def _execute_sql(self, sql_text: str, show_sql: bool, output_file: Optional[str]) -> Tuple[str, str]:
        """Выполняет SQL, возвращает (reply, error)."""
        cfg = self.config_store.load()
        if not cfg.is_complete():
            return "Нет настроек БД. Запусти команду config.", ""
        
        engine = get_engine(DBConfig(cfg.user_id, cfg.host, cfg.port, cfg.base))
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql_text))
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = result.keys()
                    # CSV с запятой для данных
                    csv_lines = [",".join(columns)]
                    for row in rows:
                        csv_lines.append(",".join(str(cell) for cell in row))
                    csv_content = "\n".join(csv_lines)
                    
                    if output_file:
                        self.fs.write_text(output_file, csv_content)
                        file_note = f"\n📁 Сохранено в: {output_file}"
                    else:
                        file_note = ""
                    
                    row_count = len(rows)
                    preview_lines = csv_lines[:21]
                    preview = "\n".join(preview_lines)
                    if row_count > 20:
                        preview += f"\n... и ещё {row_count - 20} записей"
                    
                    reply = f"Найдено {row_count} записей.{file_note}\n\n```\n{preview}\n```"
                else:
                    reply = "✅ SQL выполнен (изменение данных)."
                
                if show_sql:
                    reply = reply + f"\n\n**SQL:**\n```sql\n{sql_text}\n```"
                
                return reply, ""
        except Exception as e:
            return "", f"SQL execution failed: {e}"

    def handle_user_message(self, user_text: str) -> str:
        self.memory.add_message("user", user_text)
        self._maybe_summarize()

        cfg = self.config_store.load()
        
        # Получаем план
        feedback = ""
        context = self._build_context(feedback)
        plan_resp = self.llm.invoke(self._plan_prompt(user_text, context))
        plan = getattr(plan_resp, "content", str(plan_resp))
        
        # Выполняем шаги
        llm_calls = 1
        executed_steps = []
        all_results = []
        last_error = ""
        
        for iteration in range(self.settings.max_iters):
            if llm_calls >= self.settings.max_llm_calls:
                break
            
            context = self._build_context(feedback)
            action_resp = self.llm.invoke(
                self._action_prompt(user_text, plan, context, executed_steps)
            )
            llm_calls += 1
            
            action_text = getattr(action_resp, "content", str(action_resp))
            action = self._json_from_response(action_text)
            
            if not action:
                last_error = "LLM did not return valid JSON"
                feedback = last_error
                continue
            
            action_type = action.get("type", "answer")
            
            # Всё выполнено
            if action_type == "done":
                break
            
            # Нужна информация
            if action_type == "question":
                reply = action.get("content", "")
                self.memory.add_message("assistant", reply)
                return reply
            
            # Файловые операции
            if action_type == "fs":
                try:
                    results = self._execute_fs_ops(action.get("fs_ops", []))
                    step_result = action.get("content", "") + "\n" + "\n".join(results)
                    executed_steps.append(f"Файловые операции: {len(action.get('fs_ops', []))} шт.")
                    all_results.append(step_result)
                except Exception as e:
                    last_error = str(e)
                    feedback = last_error
                    continue
                # Продолжаем выполнять следующие шаги
                continue
            
            # SQL
            if action_type == "sql":
                sql_text = action.get("sql", "").strip()
                if not sql_text:
                    last_error = "SQL missing"
                    feedback = last_error
                    continue
                if not cfg.is_complete():
                    reply = "Нет настроек БД. Запусти команду config."
                    self.memory.add_message("assistant", reply)
                    return reply
                
                engine = get_engine(DBConfig(cfg.user_id, cfg.host, cfg.port, cfg.base))
                ok, err = validate_sql(engine, sql_text)
                if not ok:
                    fix_resp = self.llm.invoke(
                        self._fix_sql_prompt(user_text, sql_text, err or "", context)
                    )
                    llm_calls += 1
                    sql_text = getattr(fix_resp, "content", str(fix_resp)).strip()
                    ok, err = validate_sql(engine, sql_text)
                    if not ok:
                        last_error = err or "SQL validation failed"
                        feedback = last_error
                        continue
                
                run_sql = bool(action.get("run_sql", False))
                show_sql = bool(action.get("show_sql", False))
                output_file = action.get("output_file", "")
                
                if run_sql:
                    reply, error = self._execute_sql(sql_text, show_sql, output_file if output_file else None)
                    if error:
                        last_error = error
                        feedback = last_error
                        continue
                    executed_steps.append(f"SQL: {sql_text[:50]}...")
                    all_results.append(reply)
                else:
                    # Только показать SQL без выполнения
                    reply = f"**SQL:**\n```sql\n{sql_text}\n```"
                    executed_steps.append(f"Показан SQL (без выполнения)")
                    all_results.append(reply)
                
                # Продолжаем
                continue
            
            # Обычный ответ
            reply = action.get("content", "")
            executed_steps.append("Ответ пользователю")
            all_results.append(reply)
        
        # Формируем итоговый ответ
        if all_results:
            final_reply = "\n\n---\n\n".join(all_results)
            self.memory.add_message("assistant", final_reply)
            return final_reply
        
        fallback = (
            "Не удалось получить корректный результат. "
            f"Последняя ошибка: {last_error}"
        )
        self.memory.add_message("assistant", fallback)
        return fallback
