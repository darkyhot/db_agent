import cmd
import os
from pathlib import Path

from .agent_core import Agent
from .config import ConfigStore, DBConfig


class AgentCLI(cmd.Cmd):
    intro = "DB Agent готов. Введите запрос или help для команд."
    prompt = "> "

    def __init__(self, workdir: Path) -> None:
        super().__init__()
        self.workdir = workdir
        self.agent = Agent(workdir)
        self.config_store = ConfigStore()

    def do_exit(self, arg):
        return True

    def do_reset(self, arg):
        self.agent.reset()
        print("Контекст сброшен.")

    def do_config(self, arg):
        user_id = input("user_id: ").strip()
        host = input("host: ").strip()
        port = input("port (default 5432): ").strip() or "5432"
        base = input("base: ").strip()
        cfg = DBConfig(user_id=user_id, host=host, port=port, base=base)
        self.config_store.save(cfg)
        print("Конфигурация сохранена.")

    def do_clear(self, arg):
        os.system("cls" if os.name == "nt" else "clear")

    def do_status(self, arg):
        print(self.agent.status())

    def default(self, line: str):
        if not line.strip():
            return
        reply = self.agent.handle_user_message(line)
        print(reply)


def run_cli() -> None:
    AgentCLI(Path.cwd()).cmdloop()
