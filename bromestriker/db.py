import sqlite3
import time
from typing import Optional, Iterable, Tuple, List
import json

class DB:
    def __init__(self, path: str):
        # The bot and the FastAPI dashboard can touch the DB from different threads.
        # Disable SQLite's thread check so the dashboard doesn't crash on any action.
        # For this project (low write concurrency) this is sufficient.
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        try:
            # Better concurrent read/write behavior
            self.conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        self._init()

    def _init(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
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

        # --- counters ---
        # Stores the channels used for server counters.
        # kind values: members, twitch, instagram, tiktok
        cur.execute("""
        CREATE TABLE IF NOT EXISTS counters (
            guild_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            category_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, kind)
        );
        """)

        # Manual overrides for counters.
        # If set, the dashboard can force a number. The bot will still fetch automatically,
        # but the effective value is:
        #   manual_override wins UNLESS the fetched value is higher.
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS counter_overrides (
            guild_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            value INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, kind)
        );
        """
        )

        # --- giveaways ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS giveaways (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            prize TEXT NOT NULL,
            description TEXT,
            max_participants INTEGER,
            end_at INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            thumbnail_name TEXT,
            winners_count INTEGER NOT NULL DEFAULT 1,
            ended INTEGER NOT NULL DEFAULT 0,
            winner_id INTEGER,
            winner_ids TEXT
        );
        """)
        # lightweight migrations (ignore if already applied)
        try:
            cur.execute("ALTER TABLE giveaways ADD COLUMN max_participants INTEGER")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE giveaways ADD COLUMN winners_count INTEGER NOT NULL DEFAULT 1")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE giveaways ADD COLUMN winner_ids TEXT")
        except Exception:
            pass
        cur.execute("""
        CREATE TABLE IF NOT EXISTS giveaway_entries (
            giveaway_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at INTEGER NOT NULL,
            PRIMARY KEY (giveaway_id, user_id)
        );
        """)


        # --- music playlists ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_by INTEGER,
            created_at INTEGER NOT NULL,
            UNIQUE(guild_id, name)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            webpage_url TEXT,
            added_by INTEGER,
            added_at INTEGER NOT NULL
        );
        """)
        self.conn.commit()

        # Best-effort migration: older DBs won't have the counters table.
        # (SQLite CREATE TABLE IF NOT EXISTS already handles this.)

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

    # --- counters ---
    def upsert_counter(self, guild_id: int, kind: str, channel_id: int, category_id: int | None = None) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO counters (guild_id, kind, channel_id, category_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, kind) DO UPDATE SET
              channel_id=excluded.channel_id,
              category_id=excluded.category_id,
              updated_at=excluded.updated_at
            """,
            (guild_id, kind, channel_id, category_id, now, now),
        )
        self.conn.commit()

    # --- counter overrides ---
    def get_counter_override(self, guild_id: int, kind: str) -> Optional[int]:
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT value FROM counter_overrides WHERE guild_id=? AND kind=?",
            (int(guild_id), str(kind)),
        ).fetchone()
        return int(row[0]) if row else None

    def set_counter_override(self, guild_id: int, kind: str, value: int) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO counter_overrides (guild_id, kind, value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, kind) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (int(guild_id), str(kind), int(value), now),
        )
        self.conn.commit()

    def clear_counter_override(self, guild_id: int, kind: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM counter_overrides WHERE guild_id=? AND kind=?",
            (int(guild_id), str(kind)),
        )
        self.conn.commit()

    def list_counter_overrides(self, guild_id: int) -> list[sqlite3.Row]:
        cur = self.conn.cursor()
        return cur.execute(
            "SELECT kind, value, updated_at FROM counter_overrides WHERE guild_id=? ORDER BY kind ASC",
            (int(guild_id),),
        ).fetchall()

    # --- playlist tracks helpers ---
    def clear_playlist_tracks(self, playlist_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM playlist_tracks WHERE playlist_id=?", (int(playlist_id),))
        self.conn.commit()

    def delete_counter(self, guild_id: int, kind: str) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM counters WHERE guild_id=? AND kind=?", (guild_id, kind))
        self.conn.commit()

    def get_counters(self, guild_id: int) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT guild_id, kind, channel_id, category_id, created_at, updated_at FROM counters WHERE guild_id=?", (guild_id,))
        return cur.fetchall()

    # --- giveaways ---
    def create_giveaway(
        self,
        *,
        guild_id: int,
        channel_id: int,
        message_id: int,
        prize: str,
        description: str | None,
        max_participants: int | None,
        end_at: int,
        created_by: int,
        thumbnail_name: str | None = None,
        winners_count: int = 1,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO giveaways (guild_id, channel_id, message_id, prize, description, max_participants, end_at, created_by, thumbnail_name, winners_count, ended)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (guild_id, channel_id, message_id, prize, description, max_participants, end_at, created_by, thumbnail_name, winners_count),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def add_giveaway_entry(self, giveaway_id: int, user_id: int) -> bool:
        """Returns True if newly added, False if already existed."""
        cur = self.conn.cursor()
        now = int(time.time())
        cur.execute(
            "INSERT OR IGNORE INTO giveaway_entries (giveaway_id, user_id, joined_at) VALUES (?, ?, ?)",
            (giveaway_id, user_id, now),
        )
        self.conn.commit()
        return cur.rowcount > 0


    def remove_giveaway_entry(self, giveaway_id: int, user_id: int) -> bool:
        """Returns True if the entry existed and was removed."""
        cur = self.conn.cursor()
        cur.execute("DELETE FROM giveaway_entries WHERE giveaway_id=? AND user_id=?", (giveaway_id, user_id))
        self.conn.commit()
        return cur.rowcount > 0
    def giveaway_entry_count(self, giveaway_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(1) AS c FROM giveaway_entries WHERE giveaway_id=?", (giveaway_id,))
        row = cur.fetchone()
        return int(row["c"]) if row else 0

    def get_giveaway(self, giveaway_id: int) -> sqlite3.Row | None:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM giveaways WHERE id=?", (giveaway_id,))
        return cur.fetchone()

    def get_active_giveaways(self, now_ts: int | None = None) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        if now_ts is None:
            cur.execute("SELECT * FROM giveaways WHERE ended=0")
        else:
            cur.execute("SELECT * FROM giveaways WHERE ended=0 AND end_at <= ?", (now_ts,))
        return cur.fetchall()

    def get_giveaway_entries(self, giveaway_id: int) -> List[int]:
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM giveaway_entries WHERE giveaway_id=?", (giveaway_id,))
        return [int(r["user_id"]) for r in cur.fetchall()]

    def end_giveaway(self, giveaway_id: int, *, winner_ids: list[int] | None) -> None:
        """Mark giveaway ended and store winners (supports multiple winners)."""
        cur = self.conn.cursor()
        winner_id = int(winner_ids[0]) if winner_ids else None
        winner_ids_json = json.dumps([int(x) for x in winner_ids]) if winner_ids else None
        cur.execute(
            "UPDATE giveaways SET ended=1, winner_id=?, winner_ids=? WHERE id=?",
            (winner_id, winner_ids_json, giveaway_id),
        )
        self.conn.commit()

    # --- playlists ---
    def get_or_create_playlist(self, guild_id: int, name: str = "default", created_by: int | None = None) -> int:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO playlists (guild_id, name, created_by, created_at) VALUES (?, ?, ?, ?)", (guild_id, name, created_by, now))
        self.conn.commit()
        row = cur.execute("SELECT id FROM playlists WHERE guild_id=? AND name=?", (guild_id, name)).fetchone()
        return int(row[0]) if row else 0

    def add_playlist_track(self, playlist_id: int, title: str, url: str, webpage_url: str | None, added_by: int | None = None) -> int:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("INSERT INTO playlist_tracks (playlist_id, title, url, webpage_url, added_by, added_at) VALUES (?, ?, ?, ?, ?, ?)", (playlist_id, title, url, webpage_url, added_by, now))
        self.conn.commit()
        return int(cur.lastrowid)

    def list_playlist_tracks(self, playlist_id: int, limit: int = 100) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        return cur.execute("SELECT id, title, url, webpage_url, added_by, added_at FROM playlist_tracks WHERE playlist_id=? ORDER BY id DESC LIMIT ?", (playlist_id, int(limit))).fetchall()
