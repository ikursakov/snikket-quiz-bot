import json
import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
import asyncio
import os
import slixmpp
import uuid


CONFIG_FILE = "config.json"
QUESTIONS_FILE = "questions.json"
DB_FILE = "quiz.db"
SESSEION_TIMEOUT_SECONDS = 15 * 60

LETTER_START = ord("A")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_jid(jid: str) -> str:
    return jid.split("/")[0].strip().lower()


def parse_command(text: str) -> Tuple[Optional[str], str]:
    text = text.strip()
    if not text.startswith("/"):
        return None, ""
    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    return cmd, arg


def option_letter(index: int) -> str:
    return chr(LETTER_START + index)


def letter_to_index(letter: str) -> Optional[int]:
    letter = letter.strip().upper()
    if len(letter) != 1 or not ("A" <= letter <= "Z"):
        return None
    return ord(letter) - LETTER_START


class QuestionBank:
    def __init__(self, path: str):
        self.path = path
        self.questions = self._load_questions(path)
        self.by_id = {q["id"]: q for q in self.questions}

    def _load_questions(self, path: str) -> list[dict]:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("questions.json must contain a list")

        ids = set()
        for i, q in enumerate(data):
            required = {"id", "category", "question", "options", "answer"}
            missing = required - set(q.keys())
            if missing:
                raise ValueError(f"Question #{i} missing keys: {sorted(missing)}")

            if q["id"] in ids:
                raise ValueError(f"Duplicate question id: {q['id']}")
            ids.add(q["id"])

            if not isinstance(q["options"], list) or len(q["options"]) < 2:
                raise ValueError(f"Question #{i} must have at least 2 options")

            if not isinstance(q["answer"], int):
                raise ValueError(f"Question #{i} answer must be an integer")

            if q["answer"] < 0 or q["answer"] >= len(q["options"]):
                raise ValueError(f"Question #{i} answer index out of range")

            if "explanation" not in q:
                q["explanation"] = ""

            if "difficulty" not in q:
                q["difficulty"] = "normal"

        return data

    def categories(self) -> list[str]:
        return sorted({q["category"] for q in self.questions})

    def get(self, question_id: str) -> Optional[dict]:
        return self.by_id.get(question_id)

    def random_question(
        self,
        category: Optional[str] = None,
        exclude_ids: Optional[set[str]] = None,
    ) -> Optional[dict]:
        exclude_ids = exclude_ids or set()
        pool = [
            q
            for q in self.questions
            if (category is None or q["category"] == category) and q["id"] not in exclude_ids
        ]
        if not pool:
            return None
        return random.choice(pool)

    def questions_for_ids(self, ids: list[str]) -> list[dict]:
        return [self.by_id[qid] for qid in ids if qid in self.by_id]


class QuizStorage:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        cur = self.conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                username TEXT NOT NULL,
                question_id TEXT NOT NULL,
                category TEXT NOT NULL,
                selected_option INTEGER NOT NULL,
                correct_option INTEGER NOT NULL,
                is_correct INTEGER NOT NULL,
                answered_at TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                username TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                category TEXT,
                current_question_id TEXT,
                mode TEXT,
                started_at TEXT NOT NULL,
                last_activity_at TEXT NOT NULL,
                awaiting_answer INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1
            )
        """)

        self.conn.commit()

    def set_session(
        self,
        username: str,
        category: Optional[str],
        current_question_id: Optional[str],
        mode: str = "normal",
        awaiting_answer: bool = True,
    ) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO sessions(username, category, current_question_id, mode, started_at, awaiting_answer)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                category=excluded.category,
                current_question_id=excluded.current_question_id,
                mode=excluded.mode,
                started_at=excluded.started_at,
                awaiting_answer=excluded.awaiting_answer
        """, (
            username,
            category,
            current_question_id,
            mode,
            utc_now_iso(),
            1 if awaiting_answer else 0,
        ))
        self.conn.commit()

    def get_session(self, username: str) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM sessions WHERE username = ?", (username,))
        return cur.fetchone()

    def clear_session(self, username: str) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM sessions WHERE username = ?", (username,))
        self.conn.commit()

    def save_answer(self, username: str, question: dict, selected_option: int, is_correct: bool) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO answers(username, question_id, category, selected_option, correct_option, is_correct, answered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            username,
            question["id"],
            question["category"],
            selected_option,
            question["answer"],
            1 if is_correct else 0,
            utc_now_iso(),
        ))
        self.conn.commit()

    def recent_question_ids(self, username: str, category: Optional[str] = None, limit: int = 10) -> list[str]:
        cur = self.conn.cursor()
        if category:
            cur.execute("""
                SELECT question_id
                FROM answers
                WHERE username = ? AND category = ?
                ORDER BY answered_at DESC
                LIMIT ?
            """, (username, category, limit))
        else:
            cur.execute("""
                SELECT question_id
                FROM answers
                WHERE username = ?
                ORDER BY answered_at DESC
                LIMIT ?
            """, (username, limit))
        return [row["question_id"] for row in cur.fetchall()]

    def wrong_question_ids(self, username: str, limit: int = 50) -> list[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT question_id
            FROM answers
            WHERE username = ? AND is_correct = 0
            ORDER BY answered_at DESC
            LIMIT ?
        """, (username, limit))
        return [row["question_id"] for row in cur.fetchall()]

    def stats_summary(self, username: str) -> dict:
        cur = self.conn.cursor()

        cur.execute("""
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(is_correct), 0) AS correct
            FROM answers
            WHERE username = ?
        """, (username,))
        row = cur.fetchone()
        total = row["total"]
        correct = row["correct"]
        wrong = total - correct
        accuracy = 100.0 * correct / total if total else 0.0

        cur.execute("""
            SELECT category,
                   COUNT(*) AS total,
                   COALESCE(SUM(is_correct), 0) AS correct
            FROM answers
            WHERE username = ?
            GROUP BY category
            ORDER BY category
        """, (username,))
        by_category = [dict(r) for r in cur.fetchall()]

        return {
            "total": total,
            "correct": correct,
            "wrong": wrong,
            "accuracy": accuracy,
            "by_category": by_category,
        }

    def recent_mistakes(self, username: str, limit: int = 5) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT question_id, selected_option, correct_option, answered_at
            FROM answers
            WHERE username = ? AND is_correct = 0
            ORDER BY answered_at DESC
            LIMIT ?
        """, (username, limit))
        return [dict(r) for r in cur.fetchall()]

    def weakest_categories(self, username: str, min_answers: int = 2, limit: int = 3) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT category,
                   COUNT(*) AS total,
                   COALESCE(SUM(is_correct), 0) AS correct
            FROM answers
            WHERE username = ?
            GROUP BY category
            HAVING COUNT(*) >= ?
        """, (username, min_answers))

        rows = []
        for r in cur.fetchall():
            total = r["total"]
            correct = r["correct"]
            acc = 100.0 * correct / total if total else 0.0
            rows.append({
                "category": r["category"],
                "total": total,
                "correct": correct,
                "accuracy": acc,
            })

        rows.sort(key=lambda x: (x["accuracy"], -x["total"], x["category"]))
        return rows[:limit]


class QuizBot(slixmpp.ClientXMPP):
    def __init__(self, config: dict, question_bank: QuestionBank, storage: QuizStorage):
        jid = config["xmpp"]["jid"]
        password = config["xmpp"]["password"]

        super().__init__(jid, password)

        self.config_data = config
        self.qb = question_bank
        self.storage = storage
        self.user_map = {
            normalize_jid(jid_): username
            for jid_, username in config["users"].items()
        }
        self.permissions = config["permissions"]

        self.add_event_handler("session_start", self.session_start)
        self.add_event_handler("message", self.on_message)

    async def session_start(self, event):
        self.send_presence()
        await self.get_roster()

    def resolve_username(self, from_jid: str) -> Optional[str]:
        return self.user_map.get(normalize_jid(from_jid))

    def can_view(self, requester: str, target: str) -> bool:
        return target in self.permissions.get(requester, [])

    async def on_message(self, msg):
        if msg["type"] not in ("chat", "normal"):
            return

        from_jid = normalize_jid(str(msg["from"]))
        username = self.resolve_username(from_jid)

        if username is None:
            self.reply(msg, "Извини, я не знаю этого пользователя. Добавь JID в config.json.")
            return

        body = msg["body"].strip()
        if not body:
            return

        cmd, arg = parse_command(body)
        if cmd:
            await self.handle_command(msg, username, cmd, arg)
            return

        await self.handle_answer(msg, username, body)

    async def handle_command(self, msg, username: str, cmd: str, arg: str):
        if cmd == "/start":
            self.reply(msg, self.text_welcome(username))
        elif cmd == "/help":
            self.reply(msg, self.text_help(username))
        elif cmd == "/categories":
            self.reply(msg, self.text_categories())
        elif cmd == "/quiz":
            await self.cmd_quiz(msg, username, arg)
        elif cmd == "/next":
            await self.cmd_next(msg, username)
        elif cmd == "/repeat_wrong":
            await self.cmd_repeat_wrong(msg, username)
        elif cmd == "/stats":
            await self.cmd_stats(msg, username, arg)
        elif cmd == "/report":
            await self.cmd_report(msg, username, arg)
        elif cmd == "/mistakes":
            await self.cmd_mistakes(msg, username, arg)
        elif cmd == "/stop":
            self.storage.clear_session(username)
            self.reply(msg, "Текущая сессия остановлена.")
        else:
            self.reply(msg, "Неизвестная команда. Напиши /help")

    def reply(self, msg, text: str):
        self.send_message(
            mto=msg["from"],
            mbody=text,
            mtype="chat",
        )

    def text_welcome(self, username: str) -> str:
        return (
            f"Привет, {username}.\n\n"
            "Я quiz-бот.\n"
            "Команды:\n"
            "/categories — список тем\n"
            "/quiz <category> — начать квиз\n"
            "/next — следующий вопрос\n"
            "/repeat_wrong — повторить ошибочные вопросы\n"
            "/stats — твоя статистика\n"
            "/help — помощь"
        )

    def text_help(self, username: str) -> str:
        lines = [
            "Доступные команды:",
            "/start",
            "/help",
            "/categories",
            "/quiz <category>",
            "/next",
            "/repeat_wrong",
            "/stats",
            "/stop",
            "",
            "Отвечать на вопрос можно одной буквой: A, B, C, D ...",
        ]

        allowed = self.permissions.get(username, [])
        others = [u for u in allowed if u != username]
        if others:
            lines.extend([
                "",
                "Команды для просмотра статистики:",
                "/stats <username>",
                "/report <username>",
                "/mistakes <username>",
            ])

        return "\n".join(lines)

    def text_categories(self) -> str:
        cats = self.qb.categories()
        if not cats:
            return "Категорий пока нет."
        return "Доступные темы:\n" + "\n".join(f"- {c}" for c in cats)

    async def cmd_quiz(self, msg, username: str, arg: str):
        category = arg.strip()
        if not category:
            self.reply(msg, "Укажи тему: /quiz <category>\nНапример: /quiz deutschland")
            return

        if category not in self.qb.categories():
            self.reply(msg, f"Неизвестная тема: {category}\n\n{self.text_categories()}")
            return

        await self.send_new_question(msg, username, category=category, mode="normal")

    async def cmd_next(self, msg, username: str):
        session = self.storage.get_session(username)
        if not session:
            self.reply(msg, "Нет активной сессии. Начни с /quiz <category> или /repeat_wrong")
            return

        mode = session["mode"] or "normal"
        category = session["category"]
        await self.send_new_question(msg, username, category=category, mode=mode)

    async def cmd_repeat_wrong(self, msg, username: str):
        wrong_ids = self.storage.wrong_question_ids(username)
        if not wrong_ids:
            self.reply(msg, "У тебя пока нет ошибочных вопросов.")
            return

        await self.send_new_question(msg, username, category=None, mode="repeat_wrong")

    async def cmd_stats(self, msg, requester: str, arg: str):
        target = arg.strip() or requester
        if not self.can_view(requester, target):
            self.reply(msg, "У тебя нет прав смотреть эту статистику.")
            return

        stats = self.storage.stats_summary(target)
        if stats["total"] == 0:
            self.reply(msg, f"У пользователя {target} пока нет ответов.")
            return

        lines = [
            f"Статистика {target}",
            "",
            f"Всего ответов: {stats['total']}",
            f"Правильных: {stats['correct']}",
            f"Неправильных: {stats['wrong']}",
            f"Точность: {stats['accuracy']:.1f}%",
        ]

        if stats["by_category"]:
            lines.append("")
            lines.append("По темам:")
            for row in stats["by_category"]:
                total = row["total"]
                correct = row["correct"]
                acc = 100.0 * correct / total if total else 0.0
                lines.append(f"- {row['category']}: {correct}/{total} ({acc:.1f}%)")

        self.reply(msg, "\n".join(lines))

    async def cmd_report(self, msg, requester: str, arg: str):
        target = arg.strip()
        if not target:
            self.reply(msg, "Укажи пользователя: /report valentin")
            return

        if not self.can_view(requester, target):
            self.reply(msg, "У тебя нет прав смотреть эту статистику.")
            return

        stats = self.storage.stats_summary(target)
        if stats["total"] == 0:
            self.reply(msg, f"У пользователя {target} пока нет ответов.")
            return

        weak = self.storage.weakest_categories(target)
        mistakes = self.storage.recent_mistakes(target, limit=5)

        lines = [
            f"Отчёт по {target}",
            "",
            f"Всего ответов: {stats['total']}",
            f"Правильных: {stats['correct']}",
            f"Неправильных: {stats['wrong']}",
            f"Точность: {stats['accuracy']:.1f}%",
        ]

        if stats["by_category"]:
            lines.extend(["", "По темам:"])
            for row in stats["by_category"]:
                total = row["total"]
                correct = row["correct"]
                acc = 100.0 * correct / total if total else 0.0
                lines.append(f"- {row['category']}: {correct}/{total} ({acc:.1f}%)")

        if weak:
            lines.extend(["", "Слабые темы:"])
            for row in weak:
                lines.append(f"- {row['category']} ({row['accuracy']:.1f}%, {row['correct']}/{row['total']})")

        if mistakes:
            lines.extend(["", "Последние ошибки:"])
            for m in mistakes:
                q = self.qb.get(m["question_id"])
                if q:
                    lines.append(f"- {q['question']}")
                else:
                    lines.append(f"- {m['question_id']}")

        self.reply(msg, "\n".join(lines))

    async def cmd_mistakes(self, msg, requester: str, arg: str):
        target = arg.strip()
        if not target:
            self.reply(msg, "Укажи пользователя: /mistakes valentin")
            return

        if not self.can_view(requester, target):
            self.reply(msg, "У тебя нет прав смотреть эту статистику.")
            return

        mistakes = self.storage.recent_mistakes(target, limit=10)
        if not mistakes:
            self.reply(msg, f"У пользователя {target} пока нет ошибок.")
            return

        lines = [f"Последние ошибки {target}:", ""]
        for m in mistakes:
            q = self.qb.get(m["question_id"])
            if not q:
                lines.append(f"- {m['question_id']}")
                continue

            selected = option_letter(m["selected_option"])
            correct = option_letter(m["correct_option"])
            lines.append(f"- {q['question']}")
            lines.append(f"  ответ: {selected}, правильно: {correct}")

        self.reply(msg, "\n".join(lines))

    async def handle_answer(self, msg, username: str, text: str):
        session = self.storage.get_session(username)
        if not session or not session["awaiting_answer"]:
            self.reply(msg, "Я не жду ответа. Начни с /quiz <category> или /repeat_wrong")
            return

        idx = letter_to_index(text)
        if idx is None:
            self.reply(msg, "Ответь одной буквой: A, B, C, D ...")
            return

        question = self.qb.get(session["current_question_id"])
        if not question:
            self.storage.clear_session(username)
            self.reply(msg, "Не удалось найти текущий вопрос. Начни заново: /quiz <category>")
            return

        if idx >= len(question["options"]):
            max_letter = option_letter(len(question["options"]) - 1)
            self.reply(msg, f"Допустимые варианты: A..{max_letter}")
            return

        is_correct = idx == question["answer"]
        self.storage.save_answer(username, question, idx, is_correct)

        explanation = question.get("explanation", "").strip()
        answer_letter = option_letter(question["answer"])
        answer_text = question["options"][question["answer"]]

        if is_correct:
            lines = [f"✅ Правильно: {answer_letter}) {answer_text}"]
        else:
            selected_letter = option_letter(idx)
            selected_text = question["options"][idx]
            lines = [
                f"❌ Неправильно.",
                f"Твой ответ: {selected_letter}) {selected_text}",
                f"Правильный ответ: {answer_letter}) {answer_text}",
            ]

        if explanation:
            lines.extend(["", explanation])

        lines.extend(["", "Напиши /next для следующего вопроса."])
        self.storage.set_session(
            username=username,
            category=session["category"],
            current_question_id=question["id"],
            mode=session["mode"] or "normal",
            awaiting_answer=False,
        )
        self.reply(msg, "\n".join(lines))

    async def send_new_question(self, msg, username: str, category: Optional[str], mode: str):
        question = None

        if mode == "repeat_wrong":
            wrong_ids = self.storage.wrong_question_ids(username, limit=100)
            recent_ids = set(self.storage.recent_question_ids(username, limit=5))
            candidates = [qid for qid in wrong_ids if qid not in recent_ids]
            if not candidates:
                candidates = wrong_ids
            questions = self.qb.questions_for_ids(candidates)
            if questions:
                question = random.choice(questions)
        else:
            recent_ids = set(self.storage.recent_question_ids(username, category=category, limit=10))
            question = self.qb.random_question(category=category, exclude_ids=recent_ids)
            if question is None:
                question = self.qb.random_question(category=category)

        if question is None:
            if mode == "repeat_wrong":
                self.reply(msg, "Не удалось выбрать вопрос из ошибочных.")
            else:
                self.reply(msg, "Не удалось выбрать вопрос. Проверь базу вопросов.")
            return

        self.storage.set_session(
            username=username,
            category=question["category"] if mode != "repeat_wrong" else None,
            current_question_id=question["id"],
            mode=mode,
            awaiting_answer=True,
        )

        lines = [
            f"Тема: {question['category']}",
            f"Сложность: {question.get('difficulty', 'normal')}",
            "",
            question["question"],
            "",
        ]

        for i, option in enumerate(question["options"]):
            lines.append(f"{option_letter(i)}) {option}")

        lines.append("")
        lines.append("Ответь одной буквой: A, B, C, D ...")
        self.reply(msg, "\n".join(lines))


def load_config(path: str) -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    password = os.environ.get("QUIZBOT_XMPP_PASSWORD")
    if not password:
        raise RuntimeError("Set QUIZBOT_XMPP_PASSWORD")
    config["xmpp"]["password"] = password
    return config


def main():
    config = load_config(CONFIG_FILE)
    qb = QuestionBank(QUESTIONS_FILE)
    storage = QuizStorage(DB_FILE)

    xmpp = QuizBot(config, qb, storage)
    xmpp.connect()
    #xmpp.process(forever=True)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
