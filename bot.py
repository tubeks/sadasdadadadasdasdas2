import asyncio
import json
import logging
import re
import random
import sqlite3
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any
import os
from collections import defaultdict

# ================== ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==================
from dotenv import load_dotenv
load_dotenv()

# ================== AIOGRAM ИМПОРТЫ ==================
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command, CommandObject
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest

# ================== НАСТРОЙКА ЛОГИРОВАНИЯ ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== КОНФИГУРАЦИЯ ==================
TOKEN = os.getenv('BOT_TOKEN', '')
if not TOKEN:
    raise ValueError("BOT_TOKEN не найден!")

ALLOWED_BOT_SENDERS = [int(x.strip()) for x in os.getenv('ALLOWED_BOT_SENDERS', '6842501686,7588258720').split(',')]
DB_PATH = os.getenv('DB_PATH', 'data/bot_data.db')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'data/backups')

os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else 'data', exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

COOLDOWNS = {
    "global": int(os.getenv('CD_GLOBAL', '2')),
    "top": int(os.getenv('CD_TOP', '5')),
    "stats": int(os.getenv('CD_STATS', '8')),
    "rules": int(os.getenv('CD_RULES', '10')),
    "help": int(os.getenv('CD_HELP', '10')),
    "start": int(os.getenv('CD_START', '15')),
    "callback": int(os.getenv('CD_CALLBACK', '3')),
    "any_message": int(os.getenv('CD_ANY_MESSAGE', '1')),
    "moderation": int(os.getenv('CD_MODERATION', '3')),
    "games": int(os.getenv('CD_GAMES', '5')),
    "future": int(os.getenv('CD_FUTURE', '10')),
    "profile": int(os.getenv('CD_PROFILE', '3')),
    "achievements": int(os.getenv('CD_ACHIEVEMENTS', '3')),
    "duel": int(os.getenv('CD_DUEL', '300')),
}

KICK_SETTINGS = {
    "default_hours": int(os.getenv('KICK_DEFAULT_HOURS', '1')),
    "max_hours": int(os.getenv('KICK_MAX_HOURS', '24')),
    "require_confirmation": os.getenv('KICK_REQUIRE_CONFIRMATION', 'True').lower() == 'true',
}

BANNED_WORDS = {
    "игил", "isis", "isil", "даиш", "хамас", "hamas", "хамасс",
    "аль-каида", "алькаида", "аль каида", "alqaeda", "al-qaeda", "al qaeda",
    "талибан", "taliban", "талиб",
}

FUTURE_PREDICTIONS = [
    "🌟 Сегодня вас ждет неожиданная встреча!",
    "💰 Финансовая удача улыбнется вам!",
    "💕 В личной жизни наступит гармония!",
    "🎯 Все начатые дела будут успешными!",
    "🌙 Вечер принесет спокойствие!",
    "📚 Полученные знания окажутся полезными!",
    "🤝 Старые друзья напомнят о себе!",
    "🎮 Удача в играх будет на вашей стороне!",
    "💪 Энергия будет бить ключом!",
    "🌈 Неожиданное событие раскрасит будни!",
]

DEFAULT_RULES = """
📋 <b>Правила чата</b>

1. Запрещен спам.
2. Запрещены матерные оскорбления.
3. Запрещено унижение администрации.
4. Запрещены политика, религия, 18+
5. Запрещена реклама.
6. Запрещено упоминание экстремистских материалов.
7. Администрация выбирает срок наказаний.
8. Запрещен слив личных данных.
9. Запрещен контент с жестокостью к животным.
"""

bot = Bot(token=TOKEN)
dp = Dispatcher()

processed_messages = set()
processed_messages_lock = asyncio.Lock()
user_cooldowns = defaultdict(dict)
active_duels = {}

# ================== БАЗА ДАННЫХ ==================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_tables()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def init_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_stats (
                chat_id TEXT, user_id TEXT, name TEXT,
                messages INTEGER DEFAULT 0, first_seen TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                chat_id TEXT, date TEXT, user_id TEXT,
                name TEXT, messages INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, date, user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT, user_id TEXT, admin_id INTEGER,
                reason TEXT, date TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS violators (
                chat_id TEXT, user_id TEXT,
                violations INTEGER DEFAULT 0, last_violation REAL, name TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rules (
                chat_id TEXT PRIMARY KEY, rules_text TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_achievements (
                chat_id TEXT, user_id TEXT, ach_id TEXT,
                ach_name TEXT, ach_description TEXT, ach_icon TEXT,
                ach_type TEXT, granted_at TEXT, granted_by INTEGER,
                PRIMARY KEY (chat_id, user_id, ach_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS custom_achievements (
                ach_id TEXT PRIMARY KEY, name TEXT, description TEXT, icon TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS duel_stats (
                user_id TEXT PRIMARY KEY, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS join_events (
                chat_id TEXT, user_id TEXT, timestamp REAL, name TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
    def update_global_stats(self, chat_id: str, user_id: str, name: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM global_stats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        row = cursor.fetchone()
        
        if row:
            cursor.execute('UPDATE global_stats SET messages = messages + 1, name = ? WHERE chat_id = ? AND user_id = ?', (name, chat_id, user_id))
        else:
            cursor.execute('INSERT INTO global_stats (chat_id, user_id, name, messages, first_seen) VALUES (?, ?, ?, 1, ?)', (chat_id, user_id, name, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def update_daily_stats(self, chat_id: str, user_id: str, name: str, date_str: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO daily_stats (chat_id, date, user_id, name, messages)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(chat_id, date, user_id) DO UPDATE SET
                messages = messages + 1, name = excluded.name
        ''', (chat_id, date_str, user_id, name))
        conn.commit()
        conn.close()
    
    def get_user_messages_total(self, chat_id: str, user_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT messages FROM global_stats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0
    
    def get_user_messages_today(self, chat_id: str, user_id: str, date_str: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT messages FROM daily_stats WHERE chat_id = ? AND date = ? AND user_id = ?', (chat_id, date_str, user_id))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0
    
    def get_user_messages_week(self, chat_id: str, user_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        cursor.execute('SELECT SUM(messages) FROM daily_stats WHERE chat_id = ? AND user_id = ? AND date >= ?', (chat_id, user_id, week_ago))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row[0] else 0
    
    def get_user_first_seen(self, chat_id: str, user_id: str) -> Optional[str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT first_seen FROM global_stats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    
    def get_top_users(self, chat_id: str, period: str = "global", limit: int = 10) -> List[Tuple[str, int]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if period == "global":
            cursor.execute('SELECT name, messages FROM global_stats WHERE chat_id = ? ORDER BY messages DESC LIMIT ?', (chat_id, limit))
        elif period == "today":
            cursor.execute('SELECT name, messages FROM daily_stats WHERE chat_id = ? AND date = ? ORDER BY messages DESC LIMIT ?', (chat_id, str(date.today()), limit))
        elif period == "week":
            week_ago = str(date.today() - timedelta(days=7))
            cursor.execute('SELECT name, SUM(messages) as total FROM daily_stats WHERE chat_id = ? AND date >= ? GROUP BY user_id, name ORDER BY total DESC LIMIT ?', (chat_id, week_ago, limit))
        else:
            conn.close()
            return []
        
        results = [(row[0], row[1]) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_user_top_position(self, chat_id: str, user_name: str) -> Optional[int]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT name, messages FROM global_stats WHERE chat_id = ? ORDER BY messages DESC', (chat_id,))
        for pos, row in enumerate(cursor.fetchall(), 1):
            if row[0] == user_name:
                conn.close()
                return pos
        conn.close()
        return None
    
    def get_total_users(self, chat_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM global_stats WHERE chat_id = ?', (chat_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_total_messages(self, chat_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT SUM(messages) FROM global_stats WHERE chat_id = ?', (chat_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else 0
    
    def get_most_active_user(self, chat_id: str) -> Optional[Tuple[str, int]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT name, messages FROM global_stats WHERE chat_id = ? ORDER BY messages DESC LIMIT 1', (chat_id,))
        row = cursor.fetchone()
        conn.close()
        return (row[0], row[1]) if row else None
    
    def add_warning(self, chat_id: str, user_id: str, admin_id: int, reason: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO warnings (chat_id, user_id, admin_id, reason, date) VALUES (?, ?, ?, ?, ?)', (chat_id, user_id, admin_id, reason, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    def get_warnings(self, chat_id: str, user_id: str) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM warnings WHERE chat_id = ? AND user_id = ? ORDER BY date DESC', (chat_id, user_id))
        rows = cursor.fetchall()
        conn.close()
        return [{"admin_id": row[3], "reason": row[4], "date": row[5]} for row in rows]
    
    def clear_warnings(self, chat_id: str, user_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM warnings WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        conn.commit()
        conn.close()
    
    def update_violator(self, chat_id: str, user_id: str, name: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT violations FROM violators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        row = cursor.fetchone()
        
        if row:
            cursor.execute('UPDATE violators SET violations = violations + 1, last_violation = ?, name = ? WHERE chat_id = ? AND user_id = ?', (datetime.now().timestamp(), name, chat_id, user_id))
        else:
            cursor.execute('INSERT INTO violators (chat_id, user_id, violations, last_violation, name) VALUES (?, ?, 1, ?, ?)', (chat_id, user_id, datetime.now().timestamp(), name))
        
        conn.commit()
        conn.close()
    
    def get_violations_count(self, chat_id: str, user_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT violations FROM violators WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0
    
    def get_rules(self, chat_id: str) -> str:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT rules_text FROM rules WHERE chat_id = ?', (chat_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else DEFAULT_RULES
    
    def set_rules(self, chat_id: str, rules_text: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO rules (chat_id, rules_text) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET rules_text = excluded.rules_text', (chat_id, rules_text))
        conn.commit()
        conn.close()
    
    def grant_achievement(self, chat_id: str, user_id: str, ach_id: str, ach_name: str, ach_description: str, ach_icon: str, ach_type: str, granted_by: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM user_achievements WHERE chat_id = ? AND user_id = ? AND ach_id = ?', (chat_id, user_id, ach_id))
        if cursor.fetchone():
            conn.close()
            return False
        
        cursor.execute('INSERT INTO user_achievements (chat_id, user_id, ach_id, ach_name, ach_description, ach_icon, ach_type, granted_at, granted_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (chat_id, user_id, ach_id, ach_name, ach_description, ach_icon, ach_type, datetime.now().isoformat(), granted_by))
        conn.commit()
        conn.close()
        return True
    
    def has_achievement(self, chat_id: str, user_id: str, ach_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM user_achievements WHERE chat_id = ? AND user_id = ? AND ach_id = ?', (chat_id, user_id, ach_id))
        row = cursor.fetchone()
        conn.close()
        return row is not None
    
    def get_user_achievements(self, chat_id: str, user_id: str) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM user_achievements WHERE chat_id = ? AND user_id = ? ORDER BY granted_at DESC', (chat_id, user_id))
        rows = cursor.fetchall()
        conn.close()
        return [{"id": row[3], "name": row[4], "description": row[5], "icon": row[6], "type": row[7], "granted_at": row[8], "granted_by": row[9]} for row in rows]
    
    def revoke_achievement(self, chat_id: str, user_id: str, ach_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM user_achievements WHERE chat_id = ? AND user_id = ? AND ach_id = ?', (chat_id, user_id, ach_id))
        conn.commit()
        conn.close()
    
    def add_custom_achievement(self, ach_id: str, name: str, description: str, icon: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO custom_achievements (ach_id, name, description, icon) VALUES (?, ?, ?, ?)', (ach_id, name, description, icon))
        conn.commit()
        conn.close()
    
    def get_custom_achievements(self) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM custom_achievements')
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: {"id": row[0], "name": row[1], "description": row[2], "icon": row[3], "type": "custom"} for row in rows}
    
    def remove_custom_achievement(self, ach_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM custom_achievements WHERE ach_id = ?', (ach_id,))
        conn.commit()
        conn.close()
    
    def update_duel_stats(self, user_id: str, is_winner: bool):
        conn = self.get_connection()
        cursor = conn.cursor()
        if is_winner:
            cursor.execute('INSERT INTO duel_stats (user_id, wins, losses) VALUES (?, 1, 0) ON CONFLICT(user_id) DO UPDATE SET wins = wins + 1', (user_id,))
        else:
            cursor.execute('INSERT INTO duel_stats (user_id, wins, losses) VALUES (?, 0, 1) ON CONFLICT(user_id) DO UPDATE SET losses = losses + 1', (user_id,))
        conn.commit()
        conn.close()
    
    def get_duel_stats(self, user_id: str) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT wins, losses FROM duel_stats WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        return {"wins": row[0] if row else 0, "losses": row[1] if row else 0}
    
    def save_join_event(self, chat_id: str, user_id: str, name: str, timestamp: float):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO join_events (chat_id, user_id, timestamp, name) VALUES (?, ?, ?, ?) ON CONFLICT(chat_id, user_id) DO UPDATE SET timestamp = excluded.timestamp, name = excluded.name', (chat_id, user_id, timestamp, name))
        conn.commit()
        conn.close()
    
    def get_recent_joiners(self, chat_id: str, hours: float) -> List[Tuple[int, str, datetime]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cutoff = datetime.now().timestamp() - (hours * 3600)
        cursor.execute('SELECT user_id, name, timestamp FROM join_events WHERE chat_id = ? AND timestamp > ? ORDER BY timestamp DESC', (chat_id, cutoff))
        rows = cursor.fetchall()
        conn.close()
        return [(int(row[0]), row[1], datetime.fromtimestamp(row[2])) for row in rows]
    
    def clear_old_join_events(self, days: int = 7):
        conn = self.get_connection()
        cursor = conn.cursor()
        cutoff = datetime.now().timestamp() - (days * 24 * 3600)
        cursor.execute('DELETE FROM join_events WHERE timestamp < ?', (cutoff,))
        conn.commit()
        conn.close()
    
    def get_all_chats(self) -> List[str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT chat_id FROM global_stats')
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]

db = Database(DB_PATH)

# ================== СИСТЕМА ДОСТИЖЕНИЙ ==================

class AchievementSystem:
    STANDARD_ACHIEVEMENTS = {
        "first_message": {"id": "first_message", "name": "👋 Первый шаг", "description": "Отправил первое сообщение", "icon": "🌱", "type": "auto"},
        "5000_messages": {"id": "5000_messages", "name": "💬 Говорун", "description": "Написал 5000 сообщений", "icon": "🗣️", "type": "auto"},
        "25000_messages": {"id": "25000_messages", "name": "⭐ Активный", "description": "Написал 25000 сообщений", "icon": "⭐", "type": "auto"},
        "50000_messages": {"id": "50000_messages", "name": "👑 Легенда", "description": "Написал 50000 сообщений", "icon": "👑", "type": "auto"},
        "first_week": {"id": "first_week", "name": "📅 Неделя", "description": "В чате 7 дней", "icon": "📆", "type": "auto"},
        "first_month": {"id": "first_month", "name": "📆 Месяц", "description": "В чате 30 дней", "icon": "🎉", "type": "auto"},
        "veteran": {"id": "veteran", "name": "⚔️ Ветеран", "description": "В чате более года", "icon": "⚔️", "type": "auto"},
        "duel_winner": {"id": "duel_winner", "name": "🔫 Дуэлянт", "description": "Победил в дуэли", "icon": "🏆", "type": "auto"},
        "duel_loser": {"id": "duel_loser", "name": "💀 Жертва", "description": "Проиграл в дуэли", "icon": "😵", "type": "auto"}
    }
    
    def __init__(self):
        self.custom_achievements = db.get_custom_achievements()
    
    def get_all_achievements(self) -> Dict:
        all_achs = self.STANDARD_ACHIEVEMENTS.copy()
        all_achs.update(self.custom_achievements)
        return all_achs
    
    def add_custom_achievement(self, ach_id: str, name: str, description: str, icon: str = "🏆") -> bool:
        if ach_id in self.STANDARD_ACHIEVEMENTS or ach_id in self.custom_achievements:
            return False
        db.add_custom_achievement(ach_id, name, description, icon)
        self.custom_achievements[ach_id] = {"id": ach_id, "name": name, "description": description, "icon": icon, "type": "custom"}
        return True
    
    def remove_custom_achievement(self, ach_id: str) -> bool:
        if ach_id in self.custom_achievements:
            db.remove_custom_achievement(ach_id)
            del self.custom_achievements[ach_id]
            return True
        return False
    
    def grant_achievement(self, chat_id: int, user_id: int, ach_id: str, granted_by: int) -> bool:
        all_achs = self.get_all_achievements()
        if ach_id not in all_achs:
            return False
        if db.has_achievement(str(chat_id), str(user_id), ach_id):
            return False
        ach = all_achs[ach_id]
        return db.grant_achievement(str(chat_id), str(user_id), ach_id, ach["name"], ach["description"], ach["icon"], ach["type"], granted_by)
    
    def revoke_achievement(self, chat_id: int, user_id: int, ach_id: str) -> bool:
        db.revoke_achievement(str(chat_id), str(user_id), ach_id)
        return True
    
    def get_user_achievements(self, chat_id: int, user_id: int) -> List[Dict]:
        return db.get_user_achievements(str(chat_id), str(user_id))
    
    def format_achievements(self, achievements: List[Dict]) -> str:
        if not achievements:
            return "📭 Пока нет достижений"
        text = ""
        auto_achs = [a for a in achievements if a.get("type") == "auto"]
        custom_achs = [a for a in achievements if a.get("type") != "auto"]
        if auto_achs:
            text += "📊 Автоматические:\n"
            for ach in auto_achs:
                date_str = datetime.fromisoformat(ach["granted_at"]).strftime("%d.%m.%Y")
                text += f"  {ach['icon']} {ach['name']} - {ach['description']} ({date_str})\n"
            text += "\n"
        if custom_achs:
            text += "🏆 Особые:\n"
            for ach in custom_achs:
                date_str = datetime.fromisoformat(ach["granted_at"]).strftime("%d.%m.%Y")
                text += f"  {ach['icon']} {ach['name']} - {ach['description']} ({date_str})\n"
        return text
    
    def check_auto_achievements(self, chat_id: int, user_id: int, profile: Dict) -> List[str]:
        granted = []
        user_achs = [a["id"] for a in self.get_user_achievements(chat_id, user_id)]
        messages = profile["messages_total"]
        
        if messages >= 1 and "first_message" not in user_achs:
            if self.grant_achievement(chat_id, user_id, "first_message", 0):
                granted.append("first_message")
        if messages >= 5000 and "5000_messages" not in user_achs:
            if self.grant_achievement(chat_id, user_id, "5000_messages", 0):
                granted.append("5000_messages")
        if messages >= 25000 and "25000_messages" not in user_achs:
            if self.grant_achievement(chat_id, user_id, "25000_messages", 0):
                granted.append("25000_messages")
        if messages >= 50000 and "50000_messages" not in user_achs:
            if self.grant_achievement(chat_id, user_id, "50000_messages", 0):
                granted.append("50000_messages")
        
        if profile.get("first_seen"):
            try:
                first_date = datetime.fromisoformat(profile["first_seen"])
                days = (datetime.now() - first_date).days
                if days >= 7 and "first_week" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "first_week", 0):
                        granted.append("first_week")
                if days >= 30 and "first_month" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "first_month", 0):
                        granted.append("first_month")
                if days >= 365 and "veteran" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "veteran", 0):
                        granted.append("veteran")
            except:
                pass
        return granted
    
    def record_duel_result(self, chat_id: int, winner_id: int, loser_id: int):
        db.update_duel_stats(str(winner_id), True)
        db.update_duel_stats(str(loser_id), False)
        if not db.has_achievement(str(chat_id), str(winner_id), "duel_winner"):
            self.grant_achievement(chat_id, winner_id, "duel_winner", 0)
        if not db.has_achievement(str(chat_id), str(loser_id), "duel_loser"):
            self.grant_achievement(chat_id, loser_id, "duel_loser", 0)
    
    def get_duel_stats(self, user_id: int) -> Dict:
        return db.get_duel_stats(str(user_id))

achievement_system = AchievementSystem()

# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==================

def is_allowed_bot_sender(user_id: int) -> bool:
    return user_id in ALLOWED_BOT_SENDERS

async def check_admin_permissions(message: Message) -> bool:
    try:
        chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return chat_member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]
    except:
        return False

def check_cooldown(user_id: int, command_type: str = "global") -> Tuple[bool, Optional[int]]:
    try:
        current_time = datetime.now().timestamp()
        last_global = user_cooldowns[user_id].get("global")
        if last_global and current_time - last_global < COOLDOWNS["global"]:
            return False, int(COOLDOWNS["global"] - (current_time - last_global))
        last_command = user_cooldowns[user_id].get(command_type)
        if last_command and current_time - last_command < COOLDOWNS.get(command_type, 5):
            return False, int(COOLDOWNS[command_type] - (current_time - last_command))
        return True, 0
    except:
        return True, 0

def update_cooldown(user_id: int, command_type: str = "global"):
    try:
        current_time = datetime.now().timestamp()
        user_cooldowns[user_id]["global"] = current_time
        user_cooldowns[user_id][command_type] = current_time
    except:
        pass

async def send_cooldown_message(message: Message, remaining_time: int, command_name: str = "команду"):
    if remaining_time <= 0:
        return
    warning_msg = await message.answer(f"⏳ Подождите {remaining_time} сек. перед использованием {command_name}", parse_mode=ParseMode.HTML)
    await asyncio.sleep(3)
    try:
        await warning_msg.delete()
    except:
        pass

def parse_time_duration(time_str: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        if not time_str:
            return 300, "5 минут"
        time_str = time_str.lower().strip()
        if time_str in ["бессрочно", "навсегда", "0", "perm"]:
            return None, "навсегда"
        patterns = [(r'^(\d+)\s*[сc]?$', 1, "секунд"), (r'^(\d+)\s*м$', 60, "минут"), (r'^(\d+)\s*ч$', 3600, "часов"), (r'^(\d+)\s*д$', 86400, "дней")]
        for pattern, multiplier, unit in patterns:
            match = re.match(pattern, time_str)
            if match:
                value = int(match.group(1))
                return value * multiplier, f"{value} {unit[:-1] if value == 1 else unit}"
        try:
            minutes = int(time_str)
            return minutes * 60, f"{minutes} минут"
        except:
            return 300, "5 минут"
    except:
        return 300, "5 минут"

async def find_user_by_identifier(message: Message, identifier: str) -> Tuple[Optional[int], Optional[str], List]:
    try:
        if message.reply_to_message:
            user = message.reply_to_message.from_user
            return user.id, user.first_name, []
        if identifier.startswith('@'):
            try:
                chat_member = await bot.get_chat_member(message.chat.id, identifier)
                if chat_member:
                    return chat_member.user.id, chat_member.user.first_name, []
            except:
                pass
            return None, None, []
        try:
            user_id = int(identifier)
            try:
                chat_member = await bot.get_chat_member(message.chat.id, user_id)
                if chat_member:
                    return user_id, chat_member.user.first_name, []
            except:
                pass
        except:
            pass
        return None, None, []
    except:
        return None, None, []

async def check_target_user(message: Message, target_user_id: int) -> Tuple[bool, str]:
    try:
        if target_user_id == message.from_user.id:
            return False, "❌ Нельзя наказать себя!"
        target = await bot.get_chat_member(message.chat.id, target_user_id)
        if target.status == ChatMemberStatus.CREATOR:
            return False, "❌ Нельзя наказать создателя!"
        if target.status == ChatMemberStatus.ADMINISTRATOR:
            return False, "❌ Нельзя наказать администратора!"
        if target.user.is_bot:
            return False, "❌ Нельзя наказать бота!"
        return True, ""
    except:
        return False, "❌ Ошибка при проверке"

async def get_user_warnings(chat_id: int, user_id: int) -> int:
    return len(db.get_warnings(str(chat_id), str(user_id)))

async def add_user_warning(chat_id: int, user_id: int, admin_id: int, reason: str) -> int:
    db.add_warning(str(chat_id), str(user_id), admin_id, reason)
    return len(db.get_warnings(str(chat_id), str(user_id)))

async def clear_user_warnings(chat_id: int, user_id: int):
    db.clear_warnings(str(chat_id), str(user_id))

def get_user_profile(chat_id: int, user_id: int, user_name: str) -> Dict:
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    
    return {
        "user_id": user_id,
        "name": user_name,
        "first_seen": db.get_user_first_seen(chat_id_str, user_id_str),
        "messages_total": db.get_user_messages_total(chat_id_str, user_id_str),
        "messages_today": db.get_user_messages_today(chat_id_str, user_id_str, str(date.today())),
        "messages_week": db.get_user_messages_week(chat_id_str, user_id_str),
        "messages_month": db.get_user_messages_week(chat_id_str, user_id_str),
        "warnings": len(db.get_warnings(chat_id_str, user_id_str)),
        "violations": db.get_violations_count(chat_id_str, user_id_str),
        "top_position": db.get_user_top_position(chat_id_str, user_name),
    }

def format_profile(profile: Dict, chat_title: str, achievements: List[Dict] = None) -> str:
    messages = profile["messages_total"]
    if messages < 1000:
        rank = "🐣 Новенький"
    elif messages < 5000:
        rank = "🌱 Активный"
    elif messages < 10000:
        rank = "⭐ Постоянный"
    elif messages < 25000:
        rank = "🌟 Ветеран"
    elif messages < 50000:
        rank = "👑 Легенда"
    else:
        rank = "🤴 Бог чата"
    
    first_seen = "Неизвестно"
    if profile["first_seen"]:
        try:
            first_date = datetime.fromisoformat(profile["first_seen"])
            first_seen = first_date.strftime("%d.%m.%Y")
        except:
            pass
    
    warnings = profile["warnings"]
    if warnings == 0:
        warn_status = "✅ Примерный"
    elif warnings < 3:
        warn_status = "⚠️ На заметке"
    elif warnings < 5:
        warn_status = "🚫 Проблемный"
    else:
        warn_status = "⛔ В бане"
    
    text = (f"👤 Профиль пользователя\n\n"
            f"• Имя: {profile['name']}\n"
            f"• ID: {profile['user_id']}\n"
            f"• Ранг: {rank}\n"
            f"• Статус: {warn_status}\n"
            f"• В чате с: {first_seen}\n\n"
            f"Статистика в {chat_title}:\n"
            f"• Всего: {profile['messages_total']}\n"
            f"• Сегодня: {profile['messages_today']}\n"
            f"• Неделя: {profile['messages_week']}\n\n"
            f"Нарушения:\n"
            f"• Предупреждений: {profile['warnings']}\n"
            f"• Нарушений: {profile['violations']}\n")
    
    if profile["top_position"]:
        text += f"• Место в топе: {profile['top_position']}\n"
    if achievements:
        text += f"\nДостижения:\n{achievement_system.format_achievements(achievements)}"
    
    return text

def update_user_stats(chat_id: int, user_id: int, user_name: str):
    try:
        user_name = user_name.replace('<', '&lt;').replace('>', '&gt;')
        db.update_global_stats(str(chat_id), str(user_id), user_name)
        db.update_daily_stats(str(chat_id), str(user_id), user_name, str(date.today()))
        
        profile = get_user_profile(chat_id, user_id, user_name)
        granted = achievement_system.check_auto_achievements(chat_id, user_id, profile)
        if granted:
            logger.info(f"Выданы достижения {user_name}: {granted}")
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")

def get_top_users(chat_id: int, period: str = "global", limit: int = 10) -> List[Tuple[str, int]]:
    return db.get_top_users(str(chat_id), period, limit)

def format_top_message(chat_id: int, period: str, top_users: List[Tuple[str, int]]) -> str:
    period_text = {"global": "🏆 За всё время", "today": "📈 За сегодня", "week": "📊 За неделю"}.get(period, "Топ")
    if not top_users:
        return f"{period_text}\n\n📭 Статистика пуста"
    text = f"{period_text}\n\n"
    for i, (name, count) in enumerate(top_users, 1):
        medal = "🥇 " if i == 1 else "🥈 " if i == 2 else "🥉 " if i == 3 else "🔸 "
        text += f"{medal}{name}: {count} сообщ.\n"
    text += f"\n📊 Всего: {sum(c for _, c in top_users)}"
    return text

def get_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="my_profile"), InlineKeyboardButton(text="🏆 Топ", callback_data="top_global")],
        [InlineKeyboardButton(text="🏅 Достижения", callback_data="my_achievements"), InlineKeyboardButton(text="📊 За неделю", callback_data="top_week")],
        [InlineKeyboardButton(text="📋 Правила", callback_data="rules"), InlineKeyboardButton(text="💬 Помощь", callback_data="help")]
    ])

def get_help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="my_profile"), InlineKeyboardButton(text="🏅 Достижения", callback_data="my_achievements")],
        [InlineKeyboardButton(text="📊 К статистике", callback_data="back_to_stats")]
    ])

async def save_join_event(chat_id: int, user_id: int, user_name: str = None):
    db.save_join_event(str(chat_id), str(user_id), user_name or "Неизвестный", datetime.now().timestamp())

async def get_recent_joiners(chat_id: int, hours: float = 1) -> List[Tuple[int, str, datetime]]:
    return db.get_recent_joiners(str(chat_id), hours)

# ================== КОМАНДА ДУЭЛИ ==================

async def cleanup_old_duel(message_id: int, delay: int):
    await asyncio.sleep(delay)
    if message_id in active_duels:
        try:
            duel = active_duels[message_id]
            await bot.edit_message_text("⌛ Время истекло. Дуэль отменена.", chat_id=duel['chat_id'], message_id=message_id)
        except:
            pass
        del active_duels[message_id]

@dp.message(Command("duel"))
async def cmd_duel(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "duel")
        if not can_use:
            await send_cooldown_message(message, remaining, "/duel")
            return
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        if not args and not has_reply:
            await message.reply("🔫 /duel @username - вызвать на дуэль\n/duel отмена - отменить", parse_mode=ParseMode.HTML)
            return
        
        if args and args[0].lower() in ["отмена", "cancel"]:
            for duel_id, duel in list(active_duels.items()):
                if duel['creator'] == message.from_user.id and duel['chat_id'] == message.chat.id:
                    del active_duels[duel_id]
                    await message.reply("✅ Дуэль отменена")
                    return
            await message.reply("❌ Нет активных дуэлей")
            return
        
        opponent_id, opponent_name = None, None
        if has_reply:
            opponent_id = message.reply_to_message.from_user.id
            opponent_name = message.reply_to_message.from_user.full_name
        else:
            user_id, user_name, _ = await find_user_by_identifier(message, args[0])
            if not user_id:
                await message.reply("❌ Пользователь не найден")
                return
            opponent_id, opponent_name = user_id, user_name
        
        if opponent_id == message.from_user.id:
            await message.reply("❌ Нельзя вызвать себя!")
            return
        
        for duel in active_duels.values():
            if duel['chat_id'] == message.chat.id:
                if duel['creator'] == message.from_user.id or duel['opponent'] == message.from_user.id:
                    await message.reply("❌ У вас уже есть дуэль!")
                    return
        
        creator_name = message.from_user.full_name
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять", callback_data="accept_duel"), InlineKeyboardButton(text="❌ Отклонить", callback_data="decline_duel")],
            [InlineKeyboardButton(text="🗑 Отменить", callback_data="cancel_duel")]
        ])
        
        duel_msg = await message.reply(f"🔫 ДУЭЛЬ!\n\n👤 {creator_name} вызывает\n👤 {opponent_name}\n\n⏳ 2 минуты на ответ", reply_markup=keyboard)
        
        active_duels[duel_msg.message_id] = {
            'creator': message.from_user.id, 'creator_name': creator_name,
            'opponent': opponent_id, 'opponent_name': opponent_name,
            'chat_id': message.chat.id, 'message_id': duel_msg.message_id
        }
        update_cooldown(message.from_user.id, "duel")
        asyncio.create_task(cleanup_old_duel(duel_msg.message_id, 120))
    except Exception as e:
        logger.error(f"Ошибка дуэли: {e}")
        await message.reply("❌ Ошибка")

@dp.callback_query(lambda c: c.data in ["accept_duel", "decline_duel", "cancel_duel"])
async def handle_duel_callback(callback: CallbackQuery):
    try:
        message_id = callback.message.message_id
        user_id = callback.from_user.id
        if message_id not in active_duels:
            await callback.answer("❌ Дуэль неактуальна", show_alert=True)
            return
        
        duel = active_duels[message_id]
        if callback.data == "accept_duel" and user_id != duel['opponent']:
            await callback.answer("❌ Только вызванный может принять!", show_alert=True)
            return
        if callback.data == "decline_duel" and user_id not in [duel['creator'], duel['opponent']]:
            await callback.answer("❌ Только участники!", show_alert=True)
            return
        if callback.data == "cancel_duel" and user_id != duel['creator']:
            await callback.answer("❌ Только создатель!", show_alert=True)
            return
        
        if callback.data == "accept_duel":
            await callback.message.edit_text(f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n⚔️ Приготовьтесь...")
            fight_keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔫 Начать", callback_data="fight_duel")]])
            await callback.message.edit_reply_markup(reply_markup=fight_keyboard)
            await callback.answer("✅ Принято! Нажмите Начать")
        elif callback.data == "decline_duel":
            await callback.message.edit_text(f"❌ Дуэль отклонена")
            del active_duels[message_id]
            await callback.answer()
        elif callback.data == "cancel_duel":
            await callback.message.edit_text("🗑 Дуэль отменена")
            del active_duels[message_id]
            await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка callback дуэли: {e}")

@dp.callback_query(F.data == "fight_duel")
async def handle_fight_duel(callback: CallbackQuery):
    try:
        message_id = callback.message.message_id
        if message_id not in active_duels:
            await callback.answer("❌ Дуэль завершена", show_alert=True)
            return
        
        duel = active_duels[message_id]
        stages = [
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n⚔️ Противники смотрят друг на друга...",
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n🤠 Каждый готовит револьвер...",
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n🎯 Прицеливаются...",
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n💥 БАХ!"
        ]
        for stage in stages:
            await callback.message.edit_text(stage)
            await asyncio.sleep(1)
        
        winner_id = random.choice([duel['creator'], duel['opponent']])
        loser_id = duel['opponent'] if winner_id == duel['creator'] else duel['creator']
        winner_name = duel['creator_name'] if winner_id == duel['creator'] else duel['opponent_name']
        loser_name = duel['opponent_name'] if winner_id == duel['creator'] else duel['creator_name']
        
        try:
            until_date = datetime.now() + timedelta(hours=1)
            await bot.restrict_chat_member(chat_id=duel['chat_id'], user_id=loser_id, permissions={"can_send_messages": False}, until_date=until_date)
            await add_user_warning(duel['chat_id'], loser_id, 0, "Проигрыш в дуэли")
            achievement_system.record_duel_result(duel['chat_id'], winner_id, loser_id)
            result_text = f"🏆 ПОБЕДИТЕЛЬ: {winner_name}!\n\n💀 Проигравший: {loser_name}\n🔇 Мут на 1 час\n\nПобедитель: {achievement_system.get_duel_stats(winner_id)['wins']} побед\nПроигравший: {achievement_system.get_duel_stats(loser_id)['losses']} поражений"
        except Exception as e:
            logger.error(f"Ошибка мута: {e}")
            result_text = f"🏆 ПОБЕДИТЕЛЬ: {winner_name}!\n\n💀 Проигравший: {loser_name}\n❌ Не удалось применить наказание"
        
        await callback.message.edit_text(result_text)
        del active_duels[message_id]
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка fight: {e}")

# ================== КОМАНДЫ ДОСТИЖЕНИЙ ==================

@dp.message(Command("achievements"))
async def cmd_achievements(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "/achievements")
            return
        update_cooldown(message.from_user.id, "achievements")
        
        target_id, target_name = message.from_user.id, message.from_user.full_name
        if message.reply_to_message:
            target_id, target_name = message.reply_to_message.from_user.id, message.reply_to_message.from_user.full_name
        elif command.args:
            user_id, user_name, _ = await find_user_by_identifier(message, command.args)
            if user_id:
                target_id, target_name = user_id, user_name
        
        achievements = achievement_system.get_user_achievements(message.chat.id, target_id)
        duel_stats = achievement_system.get_duel_stats(target_id)
        
        if not achievements and duel_stats['wins'] == 0:
            await message.reply(f"📭 У {target_name} пока нет достижений")
            return
        
        text = f"🏆 Достижения {target_name}\n\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n{achievement_system.format_achievements(achievements)}"
        await message.reply(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка achievements: {e}")

@dp.message(Command("grant_ach"))
async def cmd_grant_achievement(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    if len(args) < 2:
        await message.reply("❌ /grant_ach @user ID_достижения")
        return
    
    target_id, target_name = None, None
    if message.reply_to_message:
        target_id, target_name = message.reply_to_message.from_user.id, message.reply_to_message.from_user.full_name
        ach_id = args[0]
    else:
        user_id, user_name, _ = await find_user_by_identifier(message, args[0])
        if not user_id:
            await message.reply("❌ Пользователь не найден")
            return
        target_id, target_name = user_id, user_name
        ach_id = args[1]
    
    if achievement_system.grant_achievement(message.chat.id, target_id, ach_id, message.from_user.id):
        ach = achievement_system.get_all_achievements().get(ach_id, {})
        await message.reply(f"✅ Выдано {ach.get('icon', '🏆')} {ach.get('name', ach_id)} пользователю {target_name}")
        try:
            await bot.send_message(target_id, f"🎉 Вам выдано достижение {ach.get('icon', '🏆')} {ach.get('name', ach_id)} в чате {message.chat.title}")
        except:
            pass
    else:
        await message.reply("❌ Не удалось выдать (возможно, уже есть)")

@dp.message(Command("revoke_ach"))
async def cmd_revoke_achievement(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    if len(args) < 2:
        await message.reply("❌ /revoke_ach @user ID_достижения")
        return
    
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        ach_id = args[0]
    else:
        user_id, _, _ = await find_user_by_identifier(message, args[0])
        if not user_id:
            await message.reply("❌ Пользователь не найден")
            return
        target_id, ach_id = user_id, args[1]
    
    if achievement_system.revoke_achievement(message.chat.id, target_id, ach_id):
        await message.reply(f"✅ Достижение отозвано")
    else:
        await message.reply("❌ Не удалось отозвать")

@dp.message(Command("create_ach"))
async def cmd_create_achievement(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split(maxsplit=3) if command.args else []
    if len(args) < 4:
        await message.reply("❌ /create_ach ID Название Иконка Описание\nПример: /create_ach best_member Лучший 👑 Самый активный")
        return
    
    if achievement_system.add_custom_achievement(args[0], args[1], args[3], args[2]):
        await message.reply(f"✅ Достижение {args[2]} {args[1]} создано")
    else:
        await message.reply("❌ ID уже существует")

@dp.message(Command("delete_ach"))
async def cmd_delete_achievement(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    
    ach_id = command.args.strip() if command.args else ""
    if not ach_id:
        await message.reply("❌ /delete_ach ID_достижения")
        return
    
    if achievement_system.remove_custom_achievement(ach_id):
        await message.reply(f"✅ Достижение {ach_id} удалено")
    else:
        await message.reply("❌ Не найдено")

@dp.message(Command("list_ach"))
async def cmd_list_achievements(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "/list_ach")
            return
        update_cooldown(message.from_user.id, "achievements")
        
        all_achs = achievement_system.get_all_achievements()
        text = "🏆 Все достижения\n\n"
        for ach in all_achs.values():
            text += f"{ach['icon']} {ach['name']} - {ach['description']}\n  ID: {ach['id']}\n\n"
        await message.reply(text[:4000], parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка list_ach: {e}")

# ================== КОМАНДА ПРОФИЛЯ ==================

@dp.message(Command("profile"))
async def cmd_profile(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "profile")
        if not can_use:
            await send_cooldown_message(message, remaining, "/profile")
            return
        update_cooldown(message.from_user.id, "profile")
        
        target_id, target_name = message.from_user.id, message.from_user.full_name
        if message.reply_to_message:
            target_id, target_name = message.reply_to_message.from_user.id, message.reply_to_message.from_user.full_name
        elif command.args:
            user_id, user_name, _ = await find_user_by_identifier(message, command.args)
            if user_id:
                target_id, target_name = user_id, user_name
        
        profile = get_user_profile(message.chat.id, target_id, target_name)
        chat = await bot.get_chat(message.chat.id)
        achievements = achievement_system.get_user_achievements(message.chat.id, target_id)
        duel_stats = achievement_system.get_duel_stats(target_id)
        
        text = format_profile(profile, chat.title or "чате", achievements)
        text += f"\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏆 Достижения", callback_data=f"ach_{target_id}")],
            [InlineKeyboardButton(text="📊 Топ", callback_data="top_global"), InlineKeyboardButton(text="📈 Моя статистика", callback_data="my_profile")]
        ])
        await message.reply(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка profile: {e}")

# ================== КОМАНДА ПРАВИЛ ==================

@dp.message(Command("rules"))
async def cmd_rules(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "rules")
        if not can_use:
            await send_cooldown_message(message, remaining, "/rules")
            return
        update_cooldown(message.from_user.id, "rules")
        
        if command.args and is_allowed_bot_sender(message.from_user.id):
            args = command.args.split(maxsplit=1)
            if len(args) < 2:
                await message.reply("❌ /rules set [текст] или /rules reset")
                return
            if args[0] == "set":
                db.set_rules(str(message.chat.id), args[1])
                await message.reply("✅ Правила обновлены")
            elif args[0] == "reset":
                db.set_rules(str(message.chat.id), DEFAULT_RULES)
                await message.reply("✅ Правила сброшены")
            else:
                await message.reply("❌ /rules set [текст] или /rules reset")
            return
        
        rules_text = db.get_rules(str(message.chat.id))
        await message.answer(rules_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка rules: {e}")

# ================== КОМАНДЫ СТАТИСТИКИ ==================

@dp.message(Command("top"))
async def cmd_top(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "top")
        if not can_use:
            await send_cooldown_message(message, remaining, "/top")
            return
        update_cooldown(message.from_user.id, "top")
        
        top_today = get_top_users(message.chat.id, "today", 10)
        text = format_top_message(message.chat.id, "today", top_today) if top_today else "📭 Статистика пуста"
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка top: {e}")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "stats")
        if not can_use:
            await send_cooldown_message(message, remaining, "/stats")
            return
        update_cooldown(message.from_user.id, "stats")
        
        chat_id = str(message.chat.id)
        total_users = db.get_total_users(chat_id)
        total_messages = db.get_total_messages(chat_id)
        most_active = db.get_most_active_user(chat_id)
        top_global = get_top_users(message.chat.id, "global", 10)
        
        text = f"📊 Статистика чата\n\n👥 Участников: {total_users}\n💬 Сообщений: {total_messages}\n"
        if most_active:
            text += f"👑 Самый активный: {most_active[0]} ({most_active[1]})\n\nТоп-10:\n"
        for i, (name, count) in enumerate(top_global, 1):
            text += f"{i}. {name}: {count}\n"
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка stats: {e}")

# ================== КОМАНДЫ МОДЕРАЦИИ ==================

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    can_use, remaining = check_cooldown(message.from_user.id, "moderation")
    if not can_use:
        await send_cooldown_message(message, remaining, "команды модерации")
        return
    update_cooldown(message.from_user.id, "moderation")
    
    args = command.args.split() if command.args else []
    if not args and not message.reply_to_message:
        await message.reply("❌ /mute @user 30м причина")
        return
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
        time_str = args[0] if args else "30м"
        reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    else:
        user_id, _, _ = await find_user_by_identifier(message, args[0])
        if not user_id:
            await message.reply("❌ Пользователь не найден")
            return
        time_str = args[1] if len(args) > 1 else "30м"
        reason = " ".join(args[2:]) if len(args) > 2 else "Не указана"
    
    can_punish, error = await check_target_user(message, user_id)
    if not can_punish:
        await message.reply(error)
        return
    
    duration, duration_text = parse_time_duration(time_str)
    try:
        permissions = {"can_send_messages": False, "can_send_media_messages": False, "can_send_polls": False, "can_send_other_messages": False}
        if duration:
            await bot.restrict_chat_member(message.chat.id, user_id, permissions, until_date=datetime.now() + timedelta(seconds=duration))
        else:
            await bot.restrict_chat_member(message.chat.id, user_id, permissions)
        
        await add_user_warning(message.chat.id, user_id, message.from_user.id, f"Мут: {reason}")
        user = await bot.get_chat_member(message.chat.id, user_id)
        await message.reply(f"✅ Заглушен {user.user.full_name}\nСрок: {duration_text}\nПричина: {reason}")
    except Exception as e:
        logger.error(f"Ошибка mute: {e}")
        await message.reply("❌ Ошибка")

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    if not args and not message.reply_to_message:
        await message.reply("❌ /unmute @user")
        return
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
    else:
        user_id, _, _ = await find_user_by_identifier(message, args[0])
    
    if not user_id:
        await message.reply("❌ Пользователь не найден")
        return
    
    try:
        permissions = {"can_send_messages": True, "can_send_media_messages": True, "can_send_polls": True, "can_send_other_messages": True}
        await bot.restrict_chat_member(message.chat.id, user_id, permissions)
        await message.reply(f"✅ Заглушка снята")
    except Exception as e:
        logger.error(f"Ошибка unmute: {e}")
        await message.reply("❌ Ошибка")

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    if not args and not message.reply_to_message:
        await message.reply("❌ /ban @user 7д причина")
        return
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
        time_str = args[0] if args else "навсегда"
        reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    else:
        user_id, _, _ = await find_user_by_identifier(message, args[0])
        if not user_id:
            await message.reply("❌ Пользователь не найден")
            return
        time_str = args[1] if len(args) > 1 else "навсегда"
        reason = " ".join(args[2:]) if len(args) > 2 else "Не указана"
    
    can_punish, error = await check_target_user(message, user_id)
    if not can_punish:
        await message.reply(error)
        return
    
    duration, duration_text = parse_time_duration(time_str)
    try:
        if duration:
            await bot.ban_chat_member(message.chat.id, user_id, until_date=datetime.now() + timedelta(seconds=duration))
        else:
            await bot.ban_chat_member(message.chat.id, user_id)
        
        await add_user_warning(message.chat.id, user_id, message.from_user.id, f"Бан: {reason}")
        user = await bot.get_chat_member(message.chat.id, user_id)
        await message.reply(f"⛔ Забанен {user.user.full_name}\nСрок: {duration_text}\nПричина: {reason}")
    except Exception as e:
        logger.error(f"Ошибка ban: {e}")
        await message.reply("❌ Ошибка")

@dp.message(Command("unban"))
async def cmd_unban(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    if not args:
        await message.reply("❌ /unban user_id")
        return
    
    try:
        user_id = int(args[0])
        await bot.unban_chat_member(message.chat.id, user_id)
        await message.reply(f"✅ Пользователь {user_id} разбанен")
    except:
        await message.reply("❌ Ошибка")

@dp.message(Command("warns"))
async def cmd_warns(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
    elif command.args:
        user_id, _, _ = await find_user_by_identifier(message, command.args)
    else:
        await message.reply("❌ Укажите пользователя")
        return
    
    if not user_id:
        await message.reply("❌ Пользователь не найден")
        return
    
    warnings = db.get_warnings(str(message.chat.id), str(user_id))
    if not warnings:
        await message.reply(f"✅ Нет предупреждений")
        return
    
    try:
        user = await bot.get_chat_member(message.chat.id, user_id)
        text = f"⚠ Предупреждения для {user.user.full_name}\n\n"
        for i, w in enumerate(warnings, 1):
            date_str = datetime.fromisoformat(w['date']).strftime("%d.%m.%Y %H:%M")
            text += f"{i}. {w['reason']}\n   🕐 {date_str}\n   👮 Админ: {w['admin_id']}\n\n"
        await message.reply(text[:4000])
    except Exception as e:
        logger.error(f"Ошибка warns: {e}")

@dp.message(Command("clearwarns"))
async def cmd_clearwarns(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
    elif command.args:
        user_id, _, _ = await find_user_by_identifier(message, command.args)
    else:
        await message.reply("❌ Укажите пользователя")
        return
    
    if not user_id:
        await message.reply("❌ Пользователь не найден")
        return
    
    await clear_user_warnings(message.chat.id, user_id)
    await message.reply(f"✅ Предупреждения очищены")

# ================== КОМАНДЫ КИКА НОВЫХ ==================

@dp.message(Command("kicknew"))
async def cmd_kick_new(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    hours = KICK_SETTINGS["default_hours"]
    for arg in args:
        try:
            hours = float(arg)
            if hours > KICK_SETTINGS["max_hours"]:
                await message.reply(f"❌ Максимум {KICK_SETTINGS['max_hours']} часов")
                return
            break
        except:
            pass
    
    recent = await get_recent_joiners(message.chat.id, hours)
    if not recent:
        await message.reply(f"✅ Новых участников за {hours} ч не найдено")
        return
    
    if KICK_SETTINGS["require_confirmation"] and "confirm" not in args:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Да, кикнуть", callback_data=f"kicknew_confirm_{hours}"), InlineKeyboardButton(text="❌ Отмена", callback_data="kicknew_cancel")]])
        await message.reply(f"⚠ Найдено {len(recent)} новых участников за {hours} ч. Кикнуть?", reply_markup=keyboard)
        return
    
    kicked = 0
    for user_id, name, _ in recent:
        try:
            await bot.ban_chat_member(message.chat.id, user_id)
            await bot.unban_chat_member(message.chat.id, user_id)
            kicked += 1
            await asyncio.sleep(0.5)
        except:
            pass
    
    await message.reply(f"✅ Кикнуто {kicked} из {len(recent)} пользователей")

@dp.callback_query(lambda c: c.data and c.data.startswith("kicknew_"))
async def handle_kicknew_callback(callback: CallbackQuery):
    if callback.data == "kicknew_cancel":
        await callback.message.edit_text("❌ Отменено")
        await callback.answer()
        return
    
    if callback.data.startswith("kicknew_confirm_"):
        hours = float(callback.data.split("_")[2])
        chat_member = await bot.get_chat_member(callback.message.chat.id, callback.from_user.id)
        if chat_member.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
            await callback.answer("❌ Нет прав", show_alert=True)
            return
        
        recent = await get_recent_joiners(callback.message.chat.id, hours)
        kicked = 0
        for user_id, name, _ in recent:
            try:
                await bot.ban_chat_member(callback.message.chat.id, user_id)
                await bot.unban_chat_member(callback.message.chat.id, user_id)
                kicked += 1
                await asyncio.sleep(0.5)
            except:
                pass
        await callback.message.edit_text(f"✅ Кикнуто {kicked} из {len(recent)} пользователей")
        await callback.answer()

@dp.message(Command("listnew"))
async def cmd_list_new(message: Message, command: CommandObject):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    args = command.args.split() if command.args else []
    hours = float(args[0]) if args else KICK_SETTINGS["default_hours"]
    recent = await get_recent_joiners(message.chat.id, hours)
    
    if not recent:
        await message.reply(f"✅ Новых участников за {hours} ч нет")
        return
    
    text = f"📊 Новые за {hours} ч ({len(recent)}):\n\n"
    for user_id, name, join_date in recent[:20]:
        text += f"• {name} (ID: {user_id}) - {join_date.strftime('%d.%m.%Y %H:%M')}\n"
    await message.reply(text)

@dp.message(Command("kicknew_clear"))
async def cmd_clear_join_events(message: Message):
    if not await check_admin_permissions(message):
        await message.reply("❌ Нет прав")
        return
    
    db.clear_old_join_events(0)
    await message.reply("✅ История событий входа очищена")

# ================== ИГРОВЫЕ КОМАНДЫ ==================

@dp.message(Command("додеп"))
async def cmd_dodep(message: Message):
    can_use, remaining = check_cooldown(message.from_user.id, "games")
    if not can_use:
        await send_cooldown_message(message, remaining, "/додеп")
        return
    update_cooldown(message.from_user.id, "games")
    await message.answer_dice(emoji="🎲")

@dp.message(Command("future"))
async def cmd_future(message: Message):
    can_use, remaining = check_cooldown(message.from_user.id, "future")
    if not can_use:
        await send_cooldown_message(message, remaining, "/future")
        return
    update_cooldown(message.from_user.id, "future")
    await message.answer(f"🔮 {random.choice(FUTURE_PREDICTIONS)}", parse_mode=ParseMode.HTML)

# ================== КОМАНДЫ ДЛЯ АДМИНОВ ==================

@dp.message(Command("say"))
async def cmd_say(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    if command.args:
        await message.answer(command.args, parse_mode=ParseMode.HTML)
        await message.delete()

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    if command.args:
        await message.answer(f"📢 {command.args}", parse_mode=ParseMode.HTML)
        await message.delete()

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    if not is_allowed_bot_sender(message.from_user.id):
        await message.reply("❌ Нет прав")
        return
    
    if not command.args:
        await message.reply("❌ Укажите текст для рассылки")
        return
    
    all_chats = db.get_all_chats()
    if not all_chats:
        await message.reply("❌ Бот не добавлен ни в один чат")
        return
    
    success = 0
    fail = 0
    for chat_id in all_chats:
        try:
            await bot.send_message(int(chat_id), command.args, parse_mode=ParseMode.HTML)
            success += 1
            await asyncio.sleep(0.1)
        except:
            fail += 1
    
    await message.reply(f"📊 Рассылка завершена\n✅ Успешно: {success}\n❌ Ошибок: {fail}")

# ================== ОСНОВНЫЕ КОМАНДЫ ==================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    can_use, remaining = check_cooldown(message.from_user.id, "start")
    if not can_use:
        await send_cooldown_message(message, remaining, "/start")
        return
    update_cooldown(message.from_user.id, "start")
    
    text = ("👋 Добро пожаловать!\n\n"
            "🎮 /додеп - кинуть кубик\n"
            "🔮 /future - предсказание\n"
            "🔫 /duel - дуэль\n\n"
            "👤 /profile - профиль\n"
            "🏅 /achievements - достижения\n"
            "📊 /top - топ активности\n"
            "📋 /rules - правила")
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

@dp.message(Command("help"))
async def cmd_help(message: Message):
    can_use, remaining = check_cooldown(message.from_user.id, "help")
    if not can_use:
        await send_cooldown_message(message, remaining, "/help")
        return
    update_cooldown(message.from_user.id, "help")
    
    text = ("📋 Команды:\n"
            "/start - меню\n"
            "/profile - профиль\n"
            "/achievements - достижения\n"
            "/top - топ\n"
            "/stats - статистика\n"
            "/rules - правила\n"
            "/додеп - кубик\n"
            "/future - предсказание\n"
            "/duel - дуэль\n\n"
            "👨‍💻 Создатель: @TEMHbIU_PRINTS")
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_help_keyboard())

# ================== ОБРАБОТЧИКИ СООБЩЕНИЙ ==================

def normalize_text(text: str) -> str:
    text = text.lower()
    for eng, rus in [('a','а'),('c','с'),('e','е'),('o','о'),('p','р'),('y','у'),('x','х')]:
        text = text.replace(eng, rus)
    return text

def contains_banned_words(text: str) -> Tuple[bool, List[str]]:
    if not text:
        return False, []
    normalized = normalize_text(text)
    found = [w for w in BANNED_WORDS if w in normalized]
    return len(found) > 0, list(set(found))

async def handle_banned_words(message: Message, found_words: List[str]):
    try:
        await message.delete()
        db.update_violator(str(message.chat.id), str(message.from_user.id), message.from_user.first_name or "Неизвестный")
        await add_user_warning(message.chat.id, message.from_user.id, 0, f"Запрещенные слова: {', '.join(found_words[:3])}")
        
        violations = db.get_violations_count(str(message.chat.id), str(message.from_user.id))
        warnings = await get_user_warnings(message.chat.id, message.from_user.id)
        
        text = f"⚠ Нарушение! Слова: {', '.join(found_words[:3])}\nНарушений: {violations}\nПредупреждений: {warnings}\n"
        if violations >= 3:
            await bot.restrict_chat_member(message.chat.id, message.from_user.id, permissions={"can_send_messages": False}, until_date=datetime.now() + timedelta(hours=24))
            text += "⏰ Мут 24 часа"
        elif violations == 2:
            text += "⚠ Последнее предупреждение!"
        else:
            text += "📝 Первое предупреждение"
        
        warn_msg = await message.answer(text, parse_mode=ParseMode.HTML)
        await asyncio.sleep(10)
        try:
            await warn_msg.delete()
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка banned_words: {e}")

@dp.message(F.new_chat_members)
async def handle_new_members(message: Message):
    for new_member in message.new_chat_members:
        if not new_member.is_bot:
            await save_join_event(message.chat.id, new_member.id, new_member.full_name)
            logger.info(f"Новый участник: {new_member.full_name}")

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    if not message.from_user or (message.text and message.text.startswith('/')):
        return
    
    can_use, _ = check_cooldown(message.from_user.id, "any_message")
    if not can_use:
        return
    update_cooldown(message.from_user.id, "any_message")
    
    text = message.text or message.caption or ""
    if text:
        has_banned, found = contains_banned_words(text)
        if has_banned:
            await handle_banned_words(message, found)
            return
    
    user_name = message.from_user.first_name or "Неизвестный"
    if message.from_user.last_name:
        user_name += f" {message.from_user.last_name}"
    update_user_stats(message.chat.id, message.from_user.id, user_name)

# ================== ПЕРИОДИЧЕСКИЕ ЗАДАЧИ ==================

async def cleanup_old_data():
    while True:
        await asyncio.sleep(3600)
        try:
            db.clear_old_join_events(7)
            logger.info("Очистка старых данных")
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")

async def connection_monitor():
    while True:
        await asyncio.sleep(60)
        try:
            await bot.get_me()
        except:
            logger.warning("Проблемы с соединением")

# ================== CALLBACK ХЕНДЛЕРЫ ==================

@dp.callback_query(F.data == "my_profile")
async def handle_my_profile(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    profile = get_user_profile(callback.message.chat.id, callback.from_user.id, callback.from_user.full_name)
    chat = await bot.get_chat(callback.message.chat.id)
    achievements = achievement_system.get_user_achievements(callback.message.chat.id, callback.from_user.id)
    duel_stats = achievement_system.get_duel_stats(callback.from_user.id)
    
    text = format_profile(profile, chat.title or "чате", achievements)
    text += f"\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений"
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "my_achievements")
async def handle_my_achievements(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    achievements = achievement_system.get_user_achievements(callback.message.chat.id, callback.from_user.id)
    duel_stats = achievement_system.get_duel_stats(callback.from_user.id)
    
    if not achievements and duel_stats['wins'] == 0:
        text = "📭 У вас пока нет достижений"
    else:
        text = f"🏆 Ваши достижения\n\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n{achievement_system.format_achievements(achievements)}"
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("top_"))
async def handle_top_callback(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    period = callback.data.split("_")[1]
    top_users = get_top_users(callback.message.chat.id, period, 10)
    text = format_top_message(callback.message.chat.id, period, top_users)
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "rules")
async def handle_rules_callback(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    rules_text = db.get_rules(str(callback.message.chat.id))
    await callback.message.edit_text(rules_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "help")
async def handle_help_callback(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    text = "💬 Создатель: @TEMHbIU_PRINTS\nТГК: https://t.me/opg_media"
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_stats")
async def handle_back_callback(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    top_today = get_top_users(callback.message.chat.id, "today", 10)
    text = format_top_message(callback.message.chat.id, "today", top_today) if top_today else "📭 Статистика пуста"
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("ach_"))
async def handle_user_achievements_callback(callback: CallbackQuery):
    can_use, remaining = check_cooldown(callback.from_user.id, "callback")
    if not can_use:
        await callback.answer(f"⏳ {remaining} сек", show_alert=False)
        return
    update_cooldown(callback.from_user.id, "callback")
    
    try:
        user_id = int(callback.data.split("_")[1])
        user = await bot.get_chat_member(callback.message.chat.id, user_id)
        achievements = achievement_system.get_user_achievements(callback.message.chat.id, user_id)
        duel_stats = achievement_system.get_duel_stats(user_id)
        
        if not achievements and duel_stats['wins'] == 0:
            text = f"📭 У {user.user.full_name} пока нет достижений"
        else:
            text = f"🏆 Достижения {user.user.full_name}\n\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n{achievement_system.format_achievements(achievements)}"
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка ach callback: {e}")
        await callback.answer("❌ Ошибка")

# ================== ЗАПУСК ==================

async def main():
    asyncio.create_task(cleanup_old_data())
    asyncio.create_task(connection_monitor())
    
    logger.info("=" * 50)
    logger.info("🤖 Бот запущен")
    logger.info(f"📁 БД: {DB_PATH}")
    logger.info("=" * 50)
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
