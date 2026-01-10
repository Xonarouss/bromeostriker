import sqlite3
import time
from typing import Optional, Iterable, Tuple, List

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS warns (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS strikes (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS mutes (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            roles_json TEXT NOT NULL,
            unmute_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            interaction_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        );
        """)
        self.conn.commit()

    # --- interaction dedupe ---
    def seen_interaction(self, interaction_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM interactions WHERE interaction_id = ?", (interaction_id,))
        return cur.fetchone() is not None

    def mark_interaction(self, interaction_id: str) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO interactions (interaction_id, created_at) VALUES (?, ?)",
                    (interaction_id, int(time.time())))
        self.conn.commit()

    def prune_interactions(self, max_age_seconds: int = 3600) -> None:
        cutoff = int(time.time()) - max_age_seconds
        cur = self.conn.cursor()
        cur.execute("DELETE FROM interactions WHERE created_at < ?", (cutoff,))
        self.conn.commit()

    # --- strikes ---
    # --- warns ---
    def get_warns(self, guild_id: int, user_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT warns FROM warns WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        row = cur.fetchone()
        return int(row["warns"]) if row else 0

    def set_warns(self, guild_id: int, user_id: int, warns: int) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO warns (guild_id, user_id, warns, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET warns=excluded.warns, updated_at=excluded.updated_at
        """, (guild_id, user_id, warns, now))
        self.conn.commit()

    def increment_warns(self, guild_id: int, user_id: int) -> int:
        w = self.get_warns(guild_id, user_id) + 1
        self.set_warns(guild_id, user_id, w)
        return w

    def decrement_warns(self, guild_id: int, user_id: int, amount: int = 1) -> int:
        w = max(0, self.get_warns(guild_id, user_id) - max(1, amount))
        self.set_warns(guild_id, user_id, w)
        return w

    def delete_warns(self, guild_id: int, user_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM warns WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        self.conn.commit()

    def get_strikes(self, guild_id: int, user_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT strikes FROM strikes WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        row = cur.fetchone()
        return int(row["strikes"]) if row else 0

    def set_strikes(self, guild_id: int, user_id: int, strikes: int) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO strikes (guild_id, user_id, strikes, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET strikes=excluded.strikes, updated_at=excluded.updated_at
        """, (guild_id, user_id, strikes, now))
        self.conn.commit()

    def increment_strikes(self, guild_id: int, user_id: int) -> int:
        s = self.get_strikes(guild_id, user_id) + 1
        self.set_strikes(guild_id, user_id, s)
        return s

    def delete_strikes(self, guild_id: int, user_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM strikes WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        self.conn.commit()

    # --- mutes ---
    def upsert_mute(self, guild_id: int, user_id: int, roles_json: str, unmute_at: int) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO mutes (guild_id, user_id, roles_json, unmute_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET roles_json=excluded.roles_json, unmute_at=excluded.unmute_at
        """, (guild_id, user_id, roles_json, unmute_at))
        self.conn.commit()

    def clear_mute(self, guild_id: int, user_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM mutes WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        self.conn.commit()

    def due_mutes(self, now_ts: int) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT guild_id, user_id, roles_json, unmute_at FROM mutes WHERE unmute_at <= ?", (now_ts,))
        return cur.fetchall()
