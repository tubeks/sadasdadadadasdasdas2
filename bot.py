import asyncio
import json
import logging
import re
import random
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Set, Any
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

# ================== КОНФИГУРАЦИЯ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==================
TOKEN = os.getenv('BOT_TOKEN', '')
if not TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

# ID пользователей, которым разрешено писать от имени бота
ALLOWED_BOT_SENDERS = [int(x.strip()) for x in os.getenv('ALLOWED_BOT_SENDERS', '6842501686,7588258720').split(',')]

# Настройки файлов
STATS_FILE = os.getenv('STATS_FILE', 'data/chat_stats.json')
RULES_FILE = os.getenv('RULES_FILE', 'data/chat_rules.json')
ACHIEVEMENTS_FILE = os.getenv('ACHIEVEMENTS_FILE', 'data/achievements.json')
JOIN_EVENTS_FILE = os.getenv('JOIN_EVENTS_FILE', 'data/join_events.json')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'data/backups')

# Создаем директории для данных
os.makedirs(os.path.dirname(STATS_FILE) if os.path.dirname(STATS_FILE) else 'data', exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# Настройки Cooldown
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

# Настройки для кика новых участников
KICK_SETTINGS = {
    "default_hours": int(os.getenv('KICK_DEFAULT_HOURS', '1')),
    "max_hours": int(os.getenv('KICK_MAX_HOURS', '24')),
    "require_confirmation": os.getenv('KICK_REQUIRE_CONFIRMATION', 'True').lower() == 'true',
}

# Запрещенные слова
BANNED_WORDS = {
    "игил", "isis", "isil", "даиш", "хамас", "hamas", "хамасс",
    "аль-каида", "алькаида", "аль каида", "alqaeda", "al-qaeda", "al qaeda",
    "талибан", "taliban", "талиб",
}

# Предсказания
FUTURE_PREDICTIONS = [
    "🌟 Сегодня вас ждет неожиданная встреча, которая изменит ваше настроение к лучшему.",
    "💰 Финансовая удача улыбнется вам - ожидайте небольшой, но приятный денежный сюрприз.",
    "💕 В личной жизни наступит гармония и взаимопонимание с близкими.",
    "🎯 Все начатые дела сегодня будут особенно успешными - дерзайте!",
    "🌙 Вечер принесет спокойствие и приятные мысли о будущем.",
    "📚 Полученные сегодня знания окажутся очень полезными в ближайшее время.",
    "🤝 Старые друзья напомнят о себе - не игнорируйте их сообщения.",
    "🎮 Удача в играх будет на вашей стороне, но знайте меру.",
    "💪 Энергия будет бить ключом - используйте ее для важных дел.",
    "🌈 Неожиданное событие раскрасит серые будни яркими красками.",
    "🍀 Вам улыбнется удача там, где вы меньше всего этого ждете.",
    "🏆 Сегодняшние усилия приведут к завтрашним победам.",
    "🎵 Музыка поможет найти решение сложной задачи.",
    "📝 Важные новости придут через сообщение или письмо.",
    "✨ Ваша интуиция сегодня особенно сильна - доверьтесь ей.",
    "🌺 Приятное знакомство перерастет в крепкую дружбу.",
    "🎁 Неожиданный подарок или сюрприз поднимет настроение.",
    "💡 Гениальная идея посетит вас в самый неожиданный момент.",
    "🏃‍♂️ Умеренная активность принесет больше пользы, чем изнурительный труд.",
    "😊 Улыбка незнакомцу вернется к вам сторицей.",
    "🔮 В ближайшие дни произойдет то, чего вы давно ждали.",
    "🎭 Сегодня вы сможете проявить свои таланты с новой стороны.",
    "🌟 Звезды благосклонны к новым начинаниям - не бойтесь пробовать.",
    "💝 Взаимность в чувствах не заставит себя долго ждать.",
    "🌿 Природа подарит вам заряд бодрости и хорошего настроения.",
    "📱 Неожиданное сообщение изменит планы на вечер.",
    "🎨 Творческий подход поможет решить старую проблему.",
    "🤔 Прежде чем принять решение, прислушайтесь к совету старших.",
    "🏠 Домашние дела будут спориться особенно легко.",
    "⚡ Быстрое решение сегодня сэкономит время завтра.",
    "🎪 Вас ждет приглашение на интересное мероприятие.",
    "💎 Ваши таланты будут замечены и оценены по достоинству.",
    "🌅 Утро задаст тон всему дню - встаньте с правильной ноги.",
    "🎯 Цель ближе, чем кажется - сделайте решительный шаг.",
    "📞 Разговор по телефону решит давний вопрос.",
    "🌟 Харизма сегодня на высоте - вы понравитесь окружающим.",
    "💭 Мысль, которая не дает покоя, приведет к важному открытию.",
    "🎪 Неожиданное приключение ждет за поворотом.",
    "💫 Мелкие неудачи обернутся большой удачей.",
    "🌺 Комплимент от незнакомца поднимет самооценку.",
    "📸 Удачный кадр сегодня станет приятным воспоминанием.",
    "🎭 Смелость в выражении чувств будет вознаграждена.",
    "🌟 Ваша доброта вернется к вам в нужный момент.",
    "💝 Романтическое настроение не покинет вас весь день.",
    "🎯 Интуиция подскажет правильный выбор.",
    "🌙 Вечерние размышления приведут к мудрому решению.",
    "🎨 Не бойтесь экспериментировать - сегодня день проб.",
    "💪 Ваша уверенность вдохновит окружающих.",
    "🎁 Судьба преподнесет приятный сюрприз.",
    "🌟 Маленькая победа сегодня - большой успех завтра."
]

# Правила чата по умолчанию
DEFAULT_RULES = """
📋 <b>Правила чата</b>

1. Запрещен спам.
2. Запрещены Любые матерные оскорбления, даже если нет конкретного адресата. Не матерные разрешаются. Оскорбления родителей и родственников запрещены.
3. Запрещено унижение модов и оригинальной новеллы. Адекватная критика приветствуется.
4. Запрещены политика, религия, 18+
5. Запрещена реклама.
6. Запрещено ЛГБТ* (Экстремисты в РФ.)
7. Запрещено упоминание экстремистких материалов, движений, террористических организаций.
8. Администрация на свое усмотрение выбирает срок наказаний за нарушение правил.
9. Запрещен слив личных данных любого рода без согласия.
10. Запрещен любой контент, в котором содержится грубое отношение к животным.
"""

# Инициализация бота
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Хранение данных
processed_messages = set()
processed_messages_lock = asyncio.Lock()
warnings_system = defaultdict(lambda: defaultdict(list))
chat_rules = defaultdict(lambda: DEFAULT_RULES)
stats = {"global": {}, "daily": {}}
violators = defaultdict(dict)
user_cooldowns = defaultdict(dict)

# ================== СИСТЕМА ДУЭЛЕЙ ==================
active_duels = {}

# ================== СИСТЕМА ДОСТИЖЕНИЙ ==================

class AchievementSystem:
    """Система достижений"""
    
    STANDARD_ACHIEVEMENTS = {
        "first_message": {
            "id": "first_message", "name": "👋 Первый шаг",
            "description": "Отправил первое сообщение в чат", "icon": "🌱", "type": "auto"
        },
        "5000_messages": {
            "id": "5000_messages", "name": "💬 Говорун",
            "description": "Написал 5000 сообщений", "icon": "🗣️", "type": "auto"
        },
        "25000_messages": {
            "id": "25000_messages", "name": "⭐ Активный",
            "description": "Написал 25000 сообщений", "icon": "⭐", "type": "auto"
        },
        "50000_messages": {
            "id": "50000_messages", "name": "👑 Легенда чата",
            "description": "Написал 50000 сообщений", "icon": "👑", "type": "auto"
        },
        "first_week": {
            "id": "first_week", "name": "📅 Неделя в чате",
            "description": "Провел в чате 7 дней", "icon": "📆", "type": "auto"
        },
        "first_month": {
            "id": "first_month", "name": "📆 Месяц с нами",
            "description": "Провел в чате 30 дней", "icon": "🎉", "type": "auto"
        },
        "veteran": {
            "id": "veteran", "name": "⚔️ Ветеран",
            "description": "В чате более года", "icon": "⚔️", "type": "auto"
        },
        "duel_winner": {
            "id": "duel_winner", "name": "🔫 Дуэлянт",
            "description": "Победил в дуэли", "icon": "🏆", "type": "auto"
        },
        "duel_loser": {
            "id": "duel_loser", "name": "💀 Жертва",
            "description": "Проиграл в дуэли", "icon": "😵", "type": "auto"
        }
    }
    
    def __init__(self):
        self.custom_achievements = {}
        self.user_achievements = defaultdict(lambda: defaultdict(list))
        self.duel_stats = defaultdict(lambda: {"wins": 0, "losses": 0})
        self.load_data()
    
    def load_data(self):
        try:
            if os.path.exists(ACHIEVEMENTS_FILE):
                with open(ACHIEVEMENTS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_achievements = data.get("custom_achievements", {})
                    loaded_achievements = data.get("user_achievements", {})
                    self.user_achievements = defaultdict(lambda: defaultdict(list))
                    for chat_id, users in loaded_achievements.items():
                        for user_id, achs in users.items():
                            self.user_achievements[chat_id][user_id] = achs
                    self.duel_stats = data.get("duel_stats", defaultdict(lambda: {"wins": 0, "losses": 0}))
                logger.info("Достижения загружены из файла")
        except Exception as e:
            logger.error(f"Ошибка при загрузке достижений: {e}")
    
    def save_data(self):
        try:
            os.makedirs(os.path.dirname(ACHIEVEMENTS_FILE), exist_ok=True)
            data_to_save = {
                "custom_achievements": self.custom_achievements,
                "user_achievements": dict(self.user_achievements),
                "duel_stats": dict(self.duel_stats)
            }
            with open(ACHIEVEMENTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            logger.info("Достижения сохранены в файл")
        except Exception as e:
            logger.error(f"Ошибка при сохранении достижений: {e}")
    
    def get_all_achievements(self) -> Dict:
        all_achs = {}
        all_achs.update(self.STANDARD_ACHIEVEMENTS)
        all_achs.update(self.custom_achievements)
        return all_achs
    
    def add_custom_achievement(self, ach_id: str, name: str, description: str, icon: str = "🏆") -> bool:
        if ach_id in self.STANDARD_ACHIEVEMENTS or ach_id in self.custom_achievements:
            return False
        
        self.custom_achievements[ach_id] = {
            "id": ach_id, "name": name, "description": description,
            "icon": icon, "type": "custom"
        }
        self.save_data()
        return True
    
    def remove_custom_achievement(self, ach_id: str) -> bool:
        if ach_id in self.custom_achievements:
            for chat_id in list(self.user_achievements.keys()):
                for user_id in list(self.user_achievements[chat_id].keys()):
                    self.user_achievements[chat_id][user_id] = [
                        a for a in self.user_achievements[chat_id][user_id]
                        if a["id"] != ach_id
                    ]
            del self.custom_achievements[ach_id]
            self.save_data()
            return True
        return False
    
    def grant_achievement(self, chat_id: int, user_id: int, ach_id: str, granted_by: int) -> bool:
        all_achs = self.get_all_achievements()
        if ach_id not in all_achs:
            return False
        
        chat_id_str = str(chat_id)
        user_id_str = str(user_id)
        
        for ach in self.user_achievements[chat_id_str][user_id_str]:
            if ach["id"] == ach_id:
                return False
        
        achievement = all_achs[ach_id].copy()
        achievement["granted_at"] = datetime.now().isoformat()
        achievement["granted_by"] = granted_by
        
        self.user_achievements[chat_id_str][user_id_str].append(achievement)
        self.save_data()
        return True
    
    def revoke_achievement(self, chat_id: int, user_id: int, ach_id: str) -> bool:
        chat_id_str = str(chat_id)
        user_id_str = str(user_id)
        
        old_count = len(self.user_achievements[chat_id_str][user_id_str])
        self.user_achievements[chat_id_str][user_id_str] = [
            ach for ach in self.user_achievements[chat_id_str][user_id_str]
            if ach["id"] != ach_id
        ]
        
        if len(self.user_achievements[chat_id_str][user_id_str]) < old_count:
            self.save_data()
            return True
        return False
    
    def get_user_achievements(self, chat_id: int, user_id: int) -> List[Dict]:
        return self.user_achievements[str(chat_id)][str(user_id)]
    
    def format_achievements(self, achievements: List[Dict]) -> str:
        if not achievements:
            return "📭 Пока нет достижений"
        
        auto_achs = []
        custom_achs = []
        
        for ach in achievements:
            if ach.get("type") == "auto":
                auto_achs.append(ach)
            else:
                custom_achs.append(ach)
        
        text = ""
        
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
                days_in_chat = (datetime.now() - first_date).days
                
                if days_in_chat >= 7 and "first_week" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "first_week", 0):
                        granted.append("first_week")
                
                if days_in_chat >= 30 and "first_month" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "first_month", 0):
                        granted.append("first_month")
                
                if days_in_chat >= 365 and "veteran" not in user_achs:
                    if self.grant_achievement(chat_id, user_id, "veteran", 0):
                        granted.append("veteran")
            except:
                pass
        
        return granted
    
    def record_duel_result(self, chat_id: int, winner_id: int, loser_id: int):
        chat_id_str = str(chat_id)
        winner_id_str = str(winner_id)
        loser_id_str = str(loser_id)
        
        if winner_id_str not in self.duel_stats:
            self.duel_stats[winner_id_str] = {"wins": 0, "losses": 0}
        if loser_id_str not in self.duel_stats:
            self.duel_stats[loser_id_str] = {"wins": 0, "losses": 0}
        
        self.duel_stats[winner_id_str]["wins"] += 1
        self.duel_stats[loser_id_str]["losses"] += 1
        
        user_achs_winner = [a["id"] for a in self.get_user_achievements(chat_id, winner_id)]
        user_achs_loser = [a["id"] for a in self.get_user_achievements(chat_id, loser_id)]
        
        if "duel_winner" not in user_achs_winner:
            self.grant_achievement(chat_id, winner_id, "duel_winner", 0)
        
        if "duel_loser" not in user_achs_loser:
            self.grant_achievement(chat_id, loser_id, "duel_loser", 0)
        
        self.save_data()
    
    def get_duel_stats(self, user_id: int) -> Dict:
        user_id_str = str(user_id)
        return self.duel_stats.get(user_id_str, {"wins": 0, "losses": 0})

achievement_system = AchievementSystem()

# ================== ФУНКЦИИ ДЛЯ РАБОТЫ С НОВЫМИ УЧАСТНИКАМИ ==================

async def save_join_event(chat_id: int, user_id: int, user_name: str = None):
    try:
        join_events = {}
        if os.path.exists(JOIN_EVENTS_FILE):
            with open(JOIN_EVENTS_FILE, 'r', encoding='utf-8') as f:
                join_events = json.load(f)
        
        chat_id_str = str(chat_id)
        user_id_str = str(user_id)
        
        if chat_id_str not in join_events:
            join_events[chat_id_str] = {}
        
        join_events[chat_id_str][user_id_str] = {
            "timestamp": datetime.now().timestamp(),
            "name": user_name or "Неизвестный"
        }
        
        cutoff_time = datetime.now().timestamp() - (7 * 24 * 3600)
        for cid in list(join_events.keys()):
            for uid in list(join_events[cid].keys()):
                if join_events[cid][uid]["timestamp"] < cutoff_time:
                    del join_events[cid][uid]
        
        with open(JOIN_EVENTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(join_events, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        logger.error(f"Ошибка при сохранении события входа: {e}")

async def get_recent_joiners(chat_id: int, hours: float = 1) -> List[Tuple[int, str, datetime]]:
    try:
        recent_joiners = []
        cutoff_time = datetime.now().timestamp() - (hours * 3600)
        chat_id_str = str(chat_id)
        
        if not os.path.exists(JOIN_EVENTS_FILE):
            return []
        
        with open(JOIN_EVENTS_FILE, 'r', encoding='utf-8') as f:
            join_events = json.load(f)
        
        if chat_id_str in join_events:
            for user_id_str, event_data in join_events[chat_id_str].items():
                join_timestamp = event_data.get("timestamp", 0)
                if join_timestamp > cutoff_time:
                    try:
                        user_id = int(user_id_str)
                        user_name = event_data.get("name", "Неизвестный")
                        join_date = datetime.fromtimestamp(join_timestamp)
                        
                        try:
                            chat_member = await bot.get_chat_member(chat_id, user_id)
                            if chat_member.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
                                recent_joiners.append((user_id, user_name, join_date))
                        except:
                            recent_joiners.append((user_id, user_name, join_date))
                    except Exception as e:
                        logger.error(f"Ошибка при обработке пользователя {user_id_str}: {e}")
        
        recent_joiners.sort(key=lambda x: x[2], reverse=True)
        return recent_joiners
        
    except Exception as e:
        logger.error(f"Ошибка при получении новых участников: {e}")
        return []

# ================== ФУНКЦИИ ДЛЯ РАБОТЫ С ДАННЫМИ ==================

def load_rules():
    global chat_rules
    try:
        if os.path.exists(RULES_FILE):
            with open(RULES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                chat_rules.clear()
                chat_rules.update(data)
            logger.info("Правила загружены из файла")
    except Exception as e:
        logger.error(f"Ошибка при загрузке правил: {e}")

def save_rules():
    try:
        os.makedirs(os.path.dirname(RULES_FILE), exist_ok=True)
        with open(RULES_FILE, 'w', encoding='utf-8') as f:
            json.dump(dict(chat_rules), f, ensure_ascii=False, indent=2)
        logger.info("Правила сохранены в файл")
    except Exception as e:
        logger.error(f"Ошибка при сохранении правил: {e}")

def load_data():
    global stats, violators, warnings_system
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                stats = data.get("stats", {"global": {}, "daily": {}})
                violators = data.get("violators", {})
                loaded_warnings = data.get("warnings_system", {})
                warnings_system = defaultdict(lambda: defaultdict(list))
                for chat_id, users in loaded_warnings.items():
                    for user_id, warns in users.items():
                        warnings_system[chat_id][user_id] = warns
            logger.info("Данные загружены из файла")
        load_rules()
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")

def save_data():
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        data_to_save = {
            "stats": stats,
            "violators": dict(violators),
            "warnings_system": dict(warnings_system)
        }
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        logger.info("Данные сохранены в файл")
        save_rules()
        achievement_system.save_data()
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных: {e}")

# ================== MIDDLEWARE ==================

async def is_message_processed(message_id: int, chat_id: int) -> bool:
    key = f"{chat_id}:{message_id}"
    async with processed_messages_lock:
        if key in processed_messages:
            return True
        processed_messages.add(key)
        asyncio.create_task(remove_processed_message(key, 10))
        return False

async def remove_processed_message(key: str, delay: int):
    await asyncio.sleep(delay)
    async with processed_messages_lock:
        processed_messages.discard(key)

class DeduplicationMiddleware(BaseMiddleware):
    def __init__(self):
        self.processed_updates = set()
        self.lock = asyncio.Lock()
    
    async def __call__(self, handler, event: Message, data: Dict[str, Any]) -> Any:
        update_id = f"{event.chat.id}:{event.message_id}"
        async with self.lock:
            if update_id in self.processed_updates:
                return
            self.processed_updates.add(update_id)
        
        result = await handler(event, data)
        asyncio.create_task(self._remove_after_delay(update_id, 10))
        return result
    
    async def _remove_after_delay(self, update_id: str, delay: int):
        await asyncio.sleep(delay)
        async with self.lock:
            self.processed_updates.discard(update_id)

dp.message.middleware(DeduplicationMiddleware())

# ================== ФУНКЦИИ ДЛЯ ПРОВЕРКИ ПРАВ ==================

def is_allowed_bot_sender(user_id: int) -> bool:
    return user_id in ALLOWED_BOT_SENDERS

async def check_admin_permissions(message: Message) -> bool:
    try:
        chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        return chat_member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]
    except Exception as e:
        logger.error(f"Ошибка при проверке прав: {e}")
        return False

# ================== СИСТЕМА COOLDOWN ==================

def check_cooldown(user_id: int, command_type: str = "global") -> Tuple[bool, Optional[int]]:
    try:
        current_time = datetime.now().timestamp()
        
        last_global = user_cooldowns[user_id].get("global")
        if last_global and current_time - last_global < COOLDOWNS["global"]:
            remaining = int(COOLDOWNS["global"] - (current_time - last_global))
            return False, remaining
        
        last_command = user_cooldowns[user_id].get(command_type)
        if last_command and current_time - last_command < COOLDOWNS.get(command_type, 5):
            remaining = int(COOLDOWNS[command_type] - (current_time - last_command))
            return False, remaining
        
        return True, 0
    except Exception as e:
        logger.error(f"Ошибка при проверке CD: {e}")
        return True, 0

def update_cooldown(user_id: int, command_type: str = "global"):
    try:
        current_time = datetime.now().timestamp()
        user_cooldowns[user_id]["global"] = current_time
        user_cooldowns[user_id][command_type] = current_time
    except Exception as e:
        logger.error(f"Ошибка при обновлении CD: {e}")

async def send_cooldown_message(message: Message, remaining_time: int, command_name: str = "команду"):
    try:
        if remaining_time <= 0:
            return
        
        time_unit = "секунд"
        if remaining_time == 1:
            time_unit = "секунду"
        elif 2 <= remaining_time <= 4:
            time_unit = "секунды"
        
        warning_msg = await message.answer(
            f"⏳ Пожалуйста, подождите {remaining_time} {time_unit} "
            f"перед использованием {command_name} снова.",
            parse_mode=ParseMode.HTML
        )
        
        await asyncio.sleep(3)
        try:
            await warning_msg.delete()
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка при отправке CD сообщения: {e}")

# ================== ФУНКЦИИ ДЛЯ ПРОФИЛЕЙ ==================

def get_user_profile(chat_id: int, user_id: int, user_name: str) -> Dict:
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    
    profile = {
        "user_id": user_id, "name": user_name, "first_seen": None,
        "messages_total": 0, "messages_today": 0, "messages_week": 0, "messages_month": 0,
        "rank": 0, "warnings": 0, "violations": 0, "last_active": None, "top_position": None,
    }
    
    if chat_id_str in stats["global"] and user_id_str in stats["global"][chat_id_str]:
        user_data = stats["global"][chat_id_str][user_id_str]
        profile["messages_total"] = user_data.get("messages", 0)
        profile["first_seen"] = user_data.get("first_seen")
    
    today = str(date.today())
    if (chat_id_str in stats["daily"] and 
        today in stats["daily"][chat_id_str] and 
        user_id_str in stats["daily"][chat_id_str][today]):
        profile["messages_today"] = stats["daily"][chat_id_str][today][user_id_str].get("messages", 0)
    
    if chat_id_str in stats["daily"]:
        week_ago = date.today() - timedelta(days=7)
        for day_str, day_data in stats["daily"][chat_id_str].items():
            try:
                day_date = datetime.strptime(day_str, "%Y-%m-%d").date()
                if day_date >= week_ago and user_id_str in day_data:
                    profile["messages_week"] += day_data[user_id_str].get("messages", 0)
            except:
                continue
    
    if chat_id_str in stats["daily"]:
        month_ago = date.today() - timedelta(days=30)
        for day_str, day_data in stats["daily"][chat_id_str].items():
            try:
                day_date = datetime.strptime(day_str, "%Y-%m-%d").date()
                if day_date >= month_ago and user_id_str in day_data:
                    profile["messages_month"] += day_data[user_id_str].get("messages", 0)
            except:
                continue
    
    profile["warnings"] = len(warnings_system[chat_id_str][user_id_str])
    
    if chat_id_str in violators and user_id_str in violators[chat_id_str]:
        profile["violations"] = violators[chat_id_str][user_id_str].get("violations", 0)
    
    profile["last_active"] = datetime.now().isoformat()
    
    if chat_id_str in stats["global"]:
        sorted_users = sorted(
            stats["global"][chat_id_str].items(),
            key=lambda x: x[1].get("messages", 0),
            reverse=True
        )
        for pos, (uid, _) in enumerate(sorted_users, 1):
            if uid == user_id_str:
                profile["top_position"] = pos
                break
    
    return profile

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
    
    profile_text = (
        f"👤 Профиль пользователя\n\n"
        f"Информация:\n"
        f"• Имя: {profile['name']}\n"
        f"• ID: {profile['user_id']}\n"
        f"• Ранг: {rank}\n"
        f"• Статус: {warn_status}\n"
        f"• В чате с: {first_seen}\n\n"
        f"Статистика в чате {chat_title}:\n"
        f"• Всего сообщений: {profile['messages_total']}\n"
        f"• За сегодня: {profile['messages_today']}\n"
        f"• За неделю: {profile['messages_week']}\n"
        f"• За месяц: {profile['messages_month']}\n\n"
        f"Нарушения:\n"
        f"• Предупреждений: {profile['warnings']}\n"
        f"• Нарушений правил: {profile['violations']}\n"
    )
    
    if profile["top_position"]:
        profile_text += f"• Место в топе: {profile['top_position']}\n"
    
    if achievements:
        profile_text += f"\nДостижения:\n"
        profile_text += achievement_system.format_achievements(achievements)
    
    return profile_text

# ================== ФУНКЦИИ ДЛЯ МОДЕРАЦИИ ==================

def parse_time_duration(time_str: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        if not time_str:
            return 300, "5 минут"
        
        time_str = time_str.lower().strip()
        
        if time_str in ["бессрочно", "навсегда", "0", "perm", "permanent"]:
            return None, "навсегда"
        
        patterns = [
            (r'^(\d+)\s*[сcек]?$', 1, "секунд"),
            (r'^(\d+)\s*м$', 60, "минут"),
            (r'^(\d+)\s*ч$', 3600, "часов"),
            (r'^(\d+)\s*д$', 86400, "дней"),
            (r'^(\d+)\s*нед$', 604800, "недель"),
            (r'^(\d+)\s*мес$', 2592000, "месяцев"),
        ]
        
        for pattern, multiplier, unit in patterns:
            match = re.match(pattern, time_str)
            if match:
                value = int(match.group(1))
                seconds = value * multiplier
                
                if value == 1:
                    unit_text = unit[:-1]
                else:
                    unit_text = unit
                
                return seconds, f"{value} {unit_text}"
        
        try:
            minutes = int(time_str)
            return minutes * 60, f"{minutes} минут"
        except ValueError:
            return 300, "5 минут"
            
    except Exception as e:
        logger.error(f"Ошибка при парсинге времени: {e}")
        return 300, "5 минут"

async def find_user_by_identifier(message: Message, identifier: str) -> Tuple[Optional[int], Optional[str], List[Tuple[int, str]]]:
    try:
        chat_id = message.chat.id
        
        if message.reply_to_message:
            user = message.reply_to_message.from_user
            return user.id, f"@{user.username}" if user.username else user.first_name, []
        
        if identifier.startswith('@'):
            username = identifier[1:].lower()
            try:
                chat_member = await bot.get_chat_member(chat_id, identifier)
                if chat_member:
                    return chat_member.user.id, f"@{chat_member.user.username}" if chat_member.user.username else chat_member.user.first_name, []
            except:
                pass
            return None, None, []
        
        try:
            user_id = int(identifier)
            try:
                chat_member = await bot.get_chat_member(chat_id, user_id)
                if chat_member:
                    return user_id, f"@{chat_member.user.username}" if chat_member.user.username else chat_member.user.first_name, []
            except:
                pass
        except ValueError:
            pass
        
        found_users = []
        chat_id_str = str(chat_id)
        if chat_id_str in stats["global"]:
            for user_id_str, user_data in stats["global"][chat_id_str].items():
                if len(found_users) >= 10:
                    break
                user_name = user_data.get("name", "").lower()
                if identifier.lower() in user_name:
                    try:
                        user_id = int(user_id_str)
                        found_users.append((user_id, user_data.get("name", "Неизвестный")))
                    except:
                        pass
        
        if len(found_users) == 1:
            return found_users[0][0], found_users[0][1], []
        elif len(found_users) > 1:
            return None, None, found_users
        
        return None, None, []
        
    except Exception as e:
        logger.error(f"Ошибка в find_user_by_identifier: {e}")
        return None, None, []

async def check_target_user(message: Message, target_user_id: int) -> Tuple[bool, str]:
    try:
        if target_user_id == message.from_user.id:
            return False, "❌ Нельзя наказать самого себя!"
        
        target_chat_member = await bot.get_chat_member(message.chat.id, target_user_id)
        
        if target_chat_member.status == ChatMemberStatus.CREATOR:
            return False, "❌ Нельзя наказать создателя чата!"
        
        if target_chat_member.status == ChatMemberStatus.ADMINISTRATOR:
            return False, "❌ Нельзя наказать администратора!"
        
        if target_chat_member.user.is_bot:
            return False, "❌ Нельзя наказать бота!"
        
        return True, ""
        
    except Exception as e:
        logger.error(f"Ошибка при проверке целевого пользователя: {e}")
        return False, "❌ Ошибка при проверке пользователя"

async def get_user_warnings(chat_id: int, user_id: int) -> int:
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    return len(warnings_system[chat_id_str][user_id_str])

async def add_user_warning(chat_id: int, user_id: int, admin_id: int, reason: str) -> int:
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    
    warning = {
        "admin_id": admin_id, "reason": reason, "date": datetime.now().isoformat()
    }
    
    warnings_system[chat_id_str][user_id_str].append(warning)
    
    if len(warnings_system[chat_id_str][user_id_str]) > 10:
        warnings_system[chat_id_str][user_id_str] = warnings_system[chat_id_str][user_id_str][-10:]
    
    save_data()
    return len(warnings_system[chat_id_str][user_id_str])

async def clear_user_warnings(chat_id: int, user_id: int):
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    
    if chat_id_str in warnings_system and user_id_str in warnings_system[chat_id_str]:
        del warnings_system[chat_id_str][user_id_str]
        save_data()

# ================== ФУНКЦИИ ДЛЯ СТАТИСТИКИ ==================

def update_user_stats(chat_id: int, user_id: int, user_name: str):
    try:
        user_name = user_name.replace('<', '&lt;').replace('>', '&gt;')
        chat_id_str = str(chat_id)
        user_id_str = str(user_id)
        today = str(date.today())
        
        if chat_id_str not in stats["global"]:
            stats["global"][chat_id_str] = {}
        
        first_seen = None
        if user_id_str not in stats["global"][chat_id_str]:
            first_seen = str(datetime.now())
            stats["global"][chat_id_str][user_id_str] = {
                "name": user_name, "messages": 1, "first_seen": first_seen
            }
        else:
            stats["global"][chat_id_str][user_id_str]["messages"] += 1
            if user_name != stats["global"][chat_id_str][user_id_str]["name"]:
                stats["global"][chat_id_str][user_id_str]["name"] = user_name
            first_seen = stats["global"][chat_id_str][user_id_str].get("first_seen")
        
        if chat_id_str not in stats["daily"]:
            stats["daily"][chat_id_str] = {}
        
        if today not in stats["daily"][chat_id_str]:
            stats["daily"][chat_id_str][today] = {}
        
        if user_id_str not in stats["daily"][chat_id_str][today]:
            stats["daily"][chat_id_str][today][user_id_str] = {
                "name": user_name, "messages": 1
            }
        else:
            stats["daily"][chat_id_str][today][user_id_str]["messages"] += 1
        
        profile = get_user_profile(chat_id, user_id, user_name)
        if first_seen:
            profile["first_seen"] = first_seen
        granted = achievement_system.check_auto_achievements(chat_id, user_id, profile)
        
        if granted:
            logger.info(f"Пользователю {user_name} ({user_id}) выданы достижения: {granted}")
        
        save_data()
    except Exception as e:
        logger.error(f"Ошибка при обновлении статистики: {e}")

def get_top_users(chat_id: int, period: str = "global", limit: int = 10) -> List[Tuple[str, int]]:
    try:
        chat_id_str = str(chat_id)
        user_stats = {}
        
        if period == "global":
            if chat_id_str in stats["global"]:
                user_stats = stats["global"][chat_id_str]
        elif period == "today":
            today = str(date.today())
            if chat_id_str in stats["daily"] and today in stats["daily"][chat_id_str]:
                user_stats = stats["daily"][chat_id_str][today]
        elif period == "yesterday":
            yesterday = str(date.today() - timedelta(days=1))
            if chat_id_str in stats["daily"] and yesterday in stats["daily"][chat_id_str]:
                user_stats = stats["daily"][chat_id_str][yesterday]
        elif period == "week":
            week_stats = {}
            if chat_id_str in stats["daily"]:
                for day in list(stats["daily"][chat_id_str].keys())[-7:]:
                    for user_id, data in stats["daily"][chat_id_str][day].items():
                        if user_id not in week_stats:
                            week_stats[user_id] = {
                                "name": data["name"], "messages": data["messages"]
                            }
                        else:
                            week_stats[user_id]["messages"] += data["messages"]
                user_stats = week_stats
        
        sorted_users = sorted(
            user_stats.items(),
            key=lambda x: x[1]["messages"],
            reverse=True
        )[:limit]
        
        return [(data["name"], data["messages"]) for _, data in sorted_users]
    except Exception as e:
        logger.error(f"Ошибка при получении топа: {e}")
        return []

def format_top_message(chat_id: int, period: str, top_users: List[Tuple[str, int]]) -> str:
    period_text = {
        "global": "🏆 Топ активности за всё время",
        "today": "📈 Топ активности за сегодня",
        "yesterday": "📉 Топ активности за вчера",
        "week": "📊 Топ активности за неделю"
    }.get(period, f"Топ активности {period}")
    
    if not top_users:
        return f"{period_text}\n\n📭 Статистика пока пуста."
    
    message = f"{period_text}\n\n"
    for i, (name, count) in enumerate(top_users, 1):
        medal = ""
        if i == 1:
            medal = "🥇 "
        elif i == 2:
            medal = "🥈 "
        elif i == 3:
            medal = "🥉 "
        else:
            medal = "🔸 "
        
        message += f"{medal}{name}: {count} сообщ.\n"
    
    total = sum(count for _, count in top_users)
    message += f"\n📊 Всего сообщений: {total}"
    
    return message

# ================== КЛАВИАТУРЫ ==================

def get_main_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="my_profile"),
            InlineKeyboardButton(text="🏆 Топ", callback_data="top_global")
        ],
        [
            InlineKeyboardButton(text="🏅 Достижения", callback_data="my_achievements"),
            InlineKeyboardButton(text="📊 За неделю", callback_data="top_week")
        ],
        [
            InlineKeyboardButton(text="📋 Правила", callback_data="rules"),
            InlineKeyboardButton(text="💬 Помощь", callback_data="help")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_help_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="my_profile"),
            InlineKeyboardButton(text="🏅 Достижения", callback_data="my_achievements")
        ],
        [
            InlineKeyboardButton(text="📊 К статистике", callback_data="back_to_stats")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ================== КОМАНДА ДУЭЛИ ==================

async def cleanup_old_duel(message_id: int, delay: int):
    await asyncio.sleep(delay)
    if message_id in active_duels:
        try:
            duel = active_duels[message_id]
            chat_id = duel['chat_id']
            await bot.edit_message_text(
                "⌛ Время на принятие вызова истекло.\nДуэль отменена.",
                chat_id=chat_id,
                message_id=message_id
            )
        except:
            pass
        del active_duels[message_id]

@dp.message(Command("duel"))
async def cmd_duel(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "duel")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /duel")
            return
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        if not args and not has_reply:
            help_text = (
                "🔫 Команда /duel\n\n"
                "Форматы использования:\n"
                "• /duel @username - вызвать на дуэль\n"
                "• /duel (ответ на сообщение) - вызвать автора\n"
                "• /duel отмена - отменить текущий вызов\n\n"
                "Правила:\n"
                "• Проигравший получает мут на 1 час\n"
                "• На принятие вызова 2 минуты\n"
                "• Кнопки могут нажимать только участники\n"
                "• Индивидуальный cooldown 5 минут"
            )
            await message.reply(help_text, parse_mode=ParseMode.HTML)
            return
        
        if args and args[0].lower() in ["отмена", "cancel"]:
            for duel_id, duel in list(active_duels.items()):
                if duel['creator'] == message.from_user.id and duel['chat_id'] == message.chat.id:
                    del active_duels[duel_id]
                    await message.reply("✅ Дуэль отменена")
                    return
            await message.reply("❌ У вас нет активных дуэлей")
            return
        
        opponent_id = None
        opponent_name = None
        
        if has_reply:
            opponent_id = message.reply_to_message.from_user.id
            opponent_name = message.reply_to_message.from_user.full_name
        else:
            user_identifier = args[0]
            user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
            
            if not user_id and found_users:
                users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
                await message.reply(
                    f"❓ Найдено несколько пользователей:\n\n{users_list}\n\n"
                    f"Уточните запрос или используйте ID."
                )
                return
            elif not user_id:
                await message.reply("❌ Пользователь не найден.")
                return
            
            opponent_id = user_id
            try:
                opponent = await bot.get_chat_member(message.chat.id, opponent_id)
                opponent_name = opponent.user.full_name
            except:
                opponent_name = user_display or "Неизвестный"
        
        if opponent_id == message.from_user.id:
            await message.reply("❌ Нельзя вызвать на дуэль самого себя!")
            return
        
        if opponent_id in [777000, 1087968824]:
            await message.reply("❌ Нельзя вызвать на дуэль Telegram или бота!")
            return
        
        try:
            opponent_member = await bot.get_chat_member(message.chat.id, opponent_id)
            if opponent_member.status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
                await message.reply("❌ Пользователь не в чате")
                return
        except:
            await message.reply("❌ Не удалось проверить пользователя")
            return
        
        for duel in active_duels.values():
            if duel['chat_id'] == message.chat.id:
                if duel['creator'] == message.from_user.id:
                    await message.reply("❌ У вас уже есть активная дуэль!")
                    return
                if duel['opponent'] == message.from_user.id:
                    await message.reply("❌ Вас уже вызвали на дуэль!")
                    return
                if duel['creator'] == opponent_id or duel['opponent'] == opponent_id:
                    await message.reply("❌ Этот пользователь уже участвует в дуэли!")
                    return
        
        creator_name = message.from_user.full_name
        
        duel_text = (
            f"🔫 ДУЭЛЬ!\n\n"
            f"👤 {creator_name} вызывает\n"
            f"👤 {opponent_name}\n\n"
            f"⚔️ Согласен ли противник на дуэль?\n\n"
            f"⏳ На принятие вызова 2 минуты"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data="accept_duel"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data="decline_duel")
            ],
            [
                InlineKeyboardButton(text="🗑 Отменить", callback_data="cancel_duel")
            ]
        ])
        
        duel_msg = await message.reply(duel_text, reply_markup=keyboard)
        
        active_duels[duel_msg.message_id] = {
            'creator': message.from_user.id,
            'creator_name': creator_name,
            'opponent': opponent_id,
            'opponent_name': opponent_name,
            'chat_id': message.chat.id,
            'message_id': duel_msg.message_id,
            'created_at': datetime.now().timestamp()
        }
        
        update_cooldown(message.from_user.id, "duel")
        asyncio.create_task(cleanup_old_duel(duel_msg.message_id, 120))
        
    except Exception as e:
        logger.error(f"Ошибка в команде /duel: {e}")
        await message.reply("❌ Произошла ошибка при создании дуэли.")

@dp.callback_query(lambda c: c.data in ["accept_duel", "decline_duel", "cancel_duel"])
async def handle_duel_callback(callback: CallbackQuery):
    try:
        message_id = callback.message.message_id
        user_id = callback.from_user.id
        
        if message_id not in active_duels:
            await callback.answer("❌ Эта дуэль уже неактуальна", show_alert=True)
            await callback.message.edit_text("⌛ Дуэль завершена или отменена")
            return
        
        duel = active_duels[message_id]
        
        if callback.data == "accept_duel":
            if user_id != duel['opponent']:
                await callback.answer("❌ Только вызванный может принять дуэль!", show_alert=True)
                return
        elif callback.data == "decline_duel":
            if user_id not in [duel['creator'], duel['opponent']]:
                await callback.answer("❌ Только участники могут отклонить дуэль!", show_alert=True)
                return
        elif callback.data == "cancel_duel":
            if user_id != duel['creator']:
                await callback.answer("❌ Только создатель может отменить дуэль!", show_alert=True)
                return
        
        if callback.data == "accept_duel":
            await callback.message.edit_text(
                f"🔫 ДУЭЛЬ НАЧИНАЕТСЯ!\n\n"
                f"👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n"
                f"⚔️ Приготовьтесь..."
            )
            
            fight_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔫 Начать дуэль", callback_data="fight_duel")]
            ])
            
            await callback.message.edit_reply_markup(reply_markup=fight_keyboard)
            await callback.answer("✅ Вы приняли вызов! Нажмите кнопку начала дуэли")
            
        elif callback.data == "decline_duel":
            decliner = "Противник" if user_id == duel['opponent'] else "Создатель"
            await callback.message.edit_text(
                f"❌ Дуэль отклонена\n\n"
                f"{decliner} отказался от дуэли."
            )
            del active_duels[message_id]
            await callback.answer()
            
        elif callback.data == "cancel_duel":
            await callback.message.edit_text("🗑 Дуэль отменена создателем.")
            del active_duels[message_id]
            await callback.answer()
            
    except Exception as e:
        logger.error(f"Ошибка в callback дуэли: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "fight_duel")
async def handle_fight_duel(callback: CallbackQuery):
    try:
        message_id = callback.message.message_id
        user_id = callback.from_user.id
        
        if message_id not in active_duels:
            await callback.answer("❌ Дуэль уже завершена", show_alert=True)
            return
        
        duel = active_duels[message_id]
        
        if user_id not in [duel['creator'], duel['opponent']]:
            await callback.answer("❌ Только участники могут начать дуэль!", show_alert=True)
            return
        
        stages = [
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n⚔️ Противники смотрят друг на друга...",
            f"🔫 ДУЭЛЬ!\n\n👤 {duel['creator_name']} VS {duel['opponent_name']}\n\n🤠 Каждый готовит свой револьвер...",
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
            permissions = {
                "can_send_messages": False,
                "can_send_media_messages": False,
                "can_send_polls": False,
                "can_send_other_messages": False,
                "can_add_web_page_previews": False,
                "can_change_info": False,
                "can_invite_users": False,
                "can_pin_messages": False
            }
            
            until_date = datetime.now() + timedelta(hours=1)
            await bot.restrict_chat_member(
                chat_id=duel['chat_id'],
                user_id=loser_id,
                permissions=permissions,
                until_date=until_date
            )
            
            await add_user_warning(duel['chat_id'], loser_id, 0, "Проигрыш в дуэли")
            achievement_system.record_duel_result(duel['chat_id'], winner_id, loser_id)
            
            result_text = (
                f"🏆 ПОБЕДИТЕЛЬ: {winner_name}!\n\n"
                f"💀 Проигравший: {loser_name}\n"
                f"🔇 Наказание: мут на 1 час\n\n"
                f"🎯 Честь и слава победителю!\n\n"
                f"📊 Статистика дуэлей:\n"
                f"Победитель: {achievement_system.get_duel_stats(winner_id)['wins']} побед\n"
                f"Проигравший: {achievement_system.get_duel_stats(loser_id)['losses']} поражений"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при муте проигравшего: {e}")
            result_text = (
                f"🏆 ПОБЕДИТЕЛЬ: {winner_name}!\n\n"
                f"💀 Проигравший: {loser_name}\n"
                f"❌ Не удалось применить наказание\n\n"
                f"🎯 Честь и слава победителю!"
            )
        
        await callback.message.edit_text(result_text)
        del active_duels[message_id]
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в fight_duel: {e}")
        await callback.answer("❌ Ошибка при проведении дуэли", show_alert=True)

# ================== КОМАНДЫ ДОСТИЖЕНИЙ ==================

@dp.message(Command("achievements"))
async def cmd_achievements(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /achievements")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        target_user_id = None
        target_user_name = None
        
        if has_reply:
            target_user_id = message.reply_to_message.from_user.id
            target_user_name = message.reply_to_message.from_user.full_name
        elif args:
            user_id, user_display, found_users = await find_user_by_identifier(message, args[0])
            
            if not user_id and found_users:
                users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
                await message.reply(
                    f"❓ Найдено несколько пользователей:\n\n{users_list}\n\n"
                    f"Уточните запрос или используйте ID."
                )
                return
            elif not user_id:
                await message.reply("❌ Пользователь не найден.")
                return
            
            target_user_id = user_id
            try:
                target_user = await bot.get_chat_member(message.chat.id, target_user_id)
                target_user_name = target_user.user.full_name
            except:
                target_user_name = user_display or "Неизвестный"
        else:
            target_user_id = message.from_user.id
            target_user_name = message.from_user.full_name
        
        achievements = achievement_system.get_user_achievements(message.chat.id, target_user_id)
        duel_stats = achievement_system.get_duel_stats(target_user_id)
        
        if not achievements and duel_stats['wins'] == 0 and duel_stats['losses'] == 0:
            await message.reply(f"📭 У пользователя {target_user_name} пока нет достижений.")
            return
        
        text = f"🏆 Достижения пользователя {target_user_name}\n\n"
        text += f"🔫 Статистика дуэлей: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n"
        text += achievement_system.format_achievements(achievements)
        
        await message.reply(text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /achievements: {e}")
        await message.reply("❌ Произошла ошибка при получении достижений.")

@dp.message(Command("grant_ach"))
async def cmd_grant_achievement(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для выдачи достижений.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /grant_ach")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        args = command.args.split() if command.args else []
        if len(args) < 2:
            help_text = (
                "🏆 Выдача достижения\n\n"
                "Форматы использования:\n"
                "• /grant_ach @username ID_достижения\n"
                "• /grant_ach (ответ на сообщение) ID_достижения\n"
                "• /grant_ach ID_пользователя ID_достижения\n\n"
                "Доступные достижения:\n"
            )
            
            all_achs = achievement_system.get_all_achievements()
            for ach_id, ach_data in all_achs.items():
                help_text += f"• {ach_data['icon']} {ach_data['name']} - {ach_id}\n"
                help_text += f"  {ach_data['description']}\n"
            
            await message.reply(help_text, parse_mode=ParseMode.HTML)
            return
        
        has_reply = message.reply_to_message is not None
        
        if has_reply:
            target_user_id = message.reply_to_message.from_user.id
            target_user_name = message.reply_to_message.from_user.full_name
            ach_id = args[0]
        else:
            user_identifier = args[0]
            ach_id = args[1]
            
            user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
            
            if not user_id and found_users:
                users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
                await message.reply(
                    f"❓ Найдено несколько пользователей:\n\n{users_list}\n\n"
                    f"Уточните запрос или используйте ID."
                )
                return
            elif not user_id:
                await message.reply("❌ Пользователь не найден.")
                return
            
            target_user_id = user_id
            try:
                target_user = await bot.get_chat_member(message.chat.id, target_user_id)
                target_user_name = target_user.user.full_name
            except:
                target_user_name = user_display or "Неизвестный"
        
        success = achievement_system.grant_achievement(
            message.chat.id, target_user_id, ach_id, message.from_user.id
        )
        
        if success:
            achievement = achievement_system.get_all_achievements().get(ach_id, {})
            await message.reply(
                f"✅ Достижение выдано!\n\n"
                f"Пользователь: {target_user_name}\n"
                f"Достижение: {achievement.get('icon', '🏆')} {achievement.get('name', ach_id)}\n"
                f"Описание: {achievement.get('description', '')}",
                parse_mode=ParseMode.HTML
            )
            
            try:
                await bot.send_message(
                    target_user_id,
                    f"🎉 Вам выдано достижение!\n\n"
                    f"Достижение: {achievement.get('icon', '🏆')} {achievement.get('name', ach_id)}\n"
                    f"Описание: {achievement.get('description', '')}\n"
                    f"В чате: {message.chat.title or 'Неизвестный чат'}",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        else:
            await message.reply(
                f"❌ Не удалось выдать достижение.\n"
                f"Возможно, у пользователя уже есть это достижение или ID достижения неверен."
            )
        
    except Exception as e:
        logger.error(f"Ошибка в команде /grant_ach: {e}")
        await message.reply("❌ Произошла ошибка при выдаче достижения.")

@dp.message(Command("revoke_ach"))
async def cmd_revoke_achievement(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для отзыва достижений.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /revoke_ach")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        args = command.args.split() if command.args else []
        if len(args) < 2:
            await message.reply("❌ Формат: /revoke_ach @username ID_достижения")
            return
        
        has_reply = message.reply_to_message is not None
        
        if has_reply:
            target_user_id = message.reply_to_message.from_user.id
            ach_id = args[0]
        else:
            user_identifier = args[0]
            ach_id = args[1]
            
            user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
            
            if not user_id:
                await message.reply("❌ Пользователь не найден.")
                return
            
            target_user_id = user_id
        
        success = achievement_system.revoke_achievement(message.chat.id, target_user_id, ach_id)
        
        if success:
            await message.reply(f"✅ Достижение отозвано.")
        else:
            await message.reply("❌ Не удалось отозвать достижение.")
        
    except Exception as e:
        logger.error(f"Ошибка в команде /revoke_ach: {e}")
        await message.reply("❌ Произошла ошибка.")

@dp.message(Command("create_ach"))
async def cmd_create_achievement(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для создания достижений.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /create_ach")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        args = command.args.split(maxsplit=3) if command.args else []
        if len(args) < 4:
            await message.reply(
                "📝 Создание достижения\n\n"
                "Формат: /create_ach ID_достижения Название Иконка Описание\n\n"
                "Пример: /create_ach best_member Лучший участник 👑 Самый активный участник месяца",
                parse_mode=ParseMode.HTML
            )
            return
        
        ach_id = args[0]
        name = args[1]
        icon = args[2]
        description = args[3]
        
        success = achievement_system.add_custom_achievement(ach_id, name, description, icon)
        
        if success:
            await message.reply(
                f"✅ Достижение создано!\n\n"
                f"ID: {ach_id}\n"
                f"Название: {icon} {name}\n"
                f"Описание: {description}",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply("❌ Достижение с таким ID уже существует.")
        
    except Exception as e:
        logger.error(f"Ошибка в команде /create_ach: {e}")
        await message.reply("❌ Произошла ошибка.")

@dp.message(Command("delete_ach"))
async def cmd_delete_achievement(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для удаления достижений.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /delete_ach")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        ach_id = command.args.strip() if command.args else ""
        if not ach_id:
            await message.reply("❌ Укажите ID достижения для удаления.")
            return
        
        success = achievement_system.remove_custom_achievement(ach_id)
        
        if success:
            await message.reply(f"✅ Достижение {ach_id} удалено.")
        else:
            await message.reply("❌ Не удалось удалить достижение (возможно, это стандартное достижение).")
        
    except Exception as e:
        logger.error(f"Ошибка в команде /delete_ach: {e}")
        await message.reply("❌ Произошла ошибка.")

@dp.message(Command("list_ach"))
async def cmd_list_achievements(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "achievements")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /list_ach")
            return
        
        update_cooldown(message.from_user.id, "achievements")
        
        all_achs = achievement_system.get_all_achievements()
        
        if not all_achs:
            await message.reply("📭 Нет доступных достижений.")
            return
        
        auto_achs = []
        custom_achs = []
        
        for ach_id, ach_data in all_achs.items():
            if ach_data.get("type") == "auto":
                auto_achs.append(ach_data)
            else:
                custom_achs.append(ach_data)
        
        text = "🏆 Все доступные достижения\n\n"
        
        if auto_achs:
            text += "📊 Автоматические:\n"
            for ach in auto_achs:
                text += f"• {ach['icon']} {ach['name']} - {ach['description']}\n"
                text += f"  {ach['id']}\n"
            text += "\n"
        
        if custom_achs:
            text += "✨ Особые:\n"
            for ach in custom_achs:
                text += f"• {ach['icon']} {ach['name']} - {ach['description']}\n"
                text += f"  {ach['id']}\n"
        
        await message.reply(text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /list_ach: {e}")
        await message.reply("❌ Произошла ошибка.")

# ================== КОМАНДА ПРОФИЛЯ ==================

@dp.message(Command("profile"))
async def cmd_profile(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "profile")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /profile")
            return
        
        update_cooldown(message.from_user.id, "profile")
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        target_user_id = None
        target_user_name = None
        
        if has_reply:
            target_user_id = message.reply_to_message.from_user.id
            target_user_name = message.reply_to_message.from_user.full_name
        elif args:
            user_id, user_display, found_users = await find_user_by_identifier(message, args[0])
            
            if not user_id and found_users:
                users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
                await message.reply(
                    f"❓ Найдено несколько пользователей:\n\n{users_list}\n\n"
                    f"Уточните запрос или используйте ID."
                )
                return
            elif not user_id:
                await message.reply("❌ Пользователь не найден.")
                return
            
            target_user_id = user_id
            try:
                target_user = await bot.get_chat_member(message.chat.id, target_user_id)
                target_user_name = target_user.user.full_name
            except:
                target_user_name = user_display or "Неизвестный"
        else:
            target_user_id = message.from_user.id
            target_user_name = message.from_user.full_name
        
        profile = get_user_profile(message.chat.id, target_user_id, target_user_name)
        
        chat = await bot.get_chat(message.chat.id)
        chat_title = chat.title or "чате"
        
        achievements = achievement_system.get_user_achievements(message.chat.id, target_user_id)
        duel_stats = achievement_system.get_duel_stats(target_user_id)
        
        profile_text = format_profile(profile, chat_title, achievements)
        profile_text += f"\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений"
        
        keyboard_buttons = []
        
        keyboard_buttons.append([
            InlineKeyboardButton(text="🏆 Достижения", callback_data=f"ach_{target_user_id}")
        ])
        
        keyboard_buttons.append([
            InlineKeyboardButton(text="📊 Топ", callback_data="top_global"),
            InlineKeyboardButton(text="📈 Моя статистика", callback_data="my_profile")
        ])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /profile: {e}")
        await message.reply("❌ Произошла ошибка при получении профиля.")

# ================== КОМАНДА ПРАВИЛ ==================

async def cmd_edit_rules(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для редактирования правил.")
            return
        
        chat_id = str(message.chat.id)
        args = command.args if command.args else ""
        
        if not args:
            current_rules = chat_rules.get(chat_id, DEFAULT_RULES)
            help_text = (
                f"📝 Редактирование правил чата\n\n"
                f"Текущие правила:\n{current_rules}\n\n"
                f"Команды:\n"
                f"/rules set [текст] - установить новые правила\n"
                f"/rules add [текст] - добавить пункт в правила\n"
                f"/rules remove [номер] - удалить пункт\n"
                f"/rules reset - сбросить на стандартные\n"
                f"/rules show - показать правила\n\n"
                f"Поддерживается HTML разметка"
            )
            await message.reply(help_text, parse_mode=ParseMode.HTML)
            return
        
        parts = args.split(maxsplit=1)
        subcommand = parts[0].lower()
        text = parts[1] if len(parts) > 1 else ""
        
        if subcommand == "set":
            if not text:
                await message.reply("❌ Укажите текст правил.")
                return
            
            chat_rules[chat_id] = text
            save_rules()
            await message.reply("✅ Правила успешно обновлены!")
            
        elif subcommand == "add":
            if not text:
                await message.reply("❌ Укажите текст нового пункта.")
                return
            
            current = chat_rules.get(chat_id, DEFAULT_RULES)
            lines = current.split('\n')
            new_lines = []
            max_num = 0
            
            for line in lines:
                new_lines.append(line)
                match = re.match(r'^(\d+)\.', line.strip())
                if match:
                    num = int(match.group(1))
                    max_num = max(max_num, num)
            
            new_lines.append(f"{max_num + 1}. {text}")
            chat_rules[chat_id] = '\n'.join(new_lines)
            save_rules()
            await message.reply(f"✅ Пункт {max_num + 1} добавлен в правила.")
            
        elif subcommand == "remove":
            if not text:
                await message.reply("❌ Укажите номер пункта для удаления.")
                return
            
            try:
                num_to_remove = int(text)
            except ValueError:
                await message.reply("❌ Укажите число - номер пункта.")
                return
            
            current = chat_rules.get(chat_id, DEFAULT_RULES)
            lines = current.split('\n')
            new_lines = []
            removed = False
            
            for line in lines:
                match = re.match(r'^(\d+)\.', line.strip())
                if match and int(match.group(1)) == num_to_remove:
                    removed = True
                    continue
                new_lines.append(line)
            
            if removed:
                final_lines = []
                num = 1
                for line in new_lines:
                    match = re.match(r'^\d+\.', line.strip())
                    if match:
                        line = line.replace(match.group(), f"{num}.", 1)
                        num += 1
                    final_lines.append(line)
                
                chat_rules[chat_id] = '\n'.join(final_lines)
                save_rules()
                await message.reply(f"✅ Пункт {num_to_remove} удален из правил.")
            else:
                await message.reply(f"❌ Пункт {num_to_remove} не найден.")
                
        elif subcommand == "reset":
            chat_rules[chat_id] = DEFAULT_RULES
            save_rules()
            await message.reply("✅ Правила сброшены на стандартные.")
            
        elif subcommand == "show":
            current = chat_rules.get(chat_id, DEFAULT_RULES)
            await message.reply(current, parse_mode=ParseMode.HTML)
            
        else:
            await message.reply("❌ Неизвестная подкоманда. Используйте /rules без аргументов для справки.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /rules edit: {e}")
        await message.reply("❌ Произошла ошибка.")

@dp.message(Command("rules"))
async def cmd_rules(message: Message, command: CommandObject):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "rules")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /rules")
            return
        
        update_cooldown(message.from_user.id, "rules")
        
        if command.args and is_allowed_bot_sender(message.from_user.id):
            await cmd_edit_rules(message, command)
            return
        
        chat_id = str(message.chat.id)
        rules_text = chat_rules.get(chat_id, DEFAULT_RULES)
        
        await message.answer(rules_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /rules: {e}")

# ================== КОМАНДЫ СТАТИСТИКИ ==================

@dp.message(Command("top"))
async def cmd_top(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "top")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /top")
            return
        
        update_cooldown(message.from_user.id, "top")
        
        chat_id = message.chat.id
        top_today = get_top_users(chat_id, "today", 10)
        
        if top_today:
            text = format_top_message(chat_id, "today", top_today)
        else:
            text = "📭 Статистика за сегодня пока пуста."
        
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в команде /top: {e}")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "stats")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /stats")
            return
        
        update_cooldown(message.from_user.id, "stats")
        
        chat_id = message.chat.id
        chat_id_str = str(chat_id)
        
        if chat_id_str not in stats["global"]:
            await message.answer("📭 Статистика пока не собрана.", reply_markup=get_main_keyboard())
            return
        
        top_global = get_top_users(chat_id, "global", 10)
        total_users = len(stats["global"][chat_id_str])
        total_messages = sum(data["messages"] for data in stats["global"][chat_id_str].values())
        
        most_active = max(stats["global"][chat_id_str].items(), key=lambda x: x[1]["messages"])
        
        stats_text = (
            f"📊 Общая статистика чата\n\n"
            f"👥 Участников: {total_users}\n"
            f"💬 Сообщений: {total_messages}\n"
            f"👑 Самый активный: {most_active[1]['name']} ({most_active[1]['messages']})\n\n"
            f"Топ-10:\n"
        )
        
        for i, (name, count) in enumerate(top_global[:10], 1):
            stats_text += f"{i}. {name}: {count}\n"
        
        await message.answer(stats_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}")

# ================== КОМАНДЫ МОДЕРАЦИИ ==================

@dp.message(Command("mute"))
async def cmd_mute(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды модерации")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        if not args and not has_reply:
            help_text = (
                "🔇 Команда /mute\n\n"
                "Форматы использования:\n"
                "• /mute @username 30м причина\n"
                "• /mute 123456789 2ч причина\n"
                "• /mute (ответ на сообщение) 1д причина\n\n"
            )
            await message.reply(help_text, parse_mode=ParseMode.HTML)
            return
        
        user_identifier = None
        time_str = None
        reason_parts = []
        
        if has_reply:
            if args:
                time_str = args[0]
                reason_parts = args[1:] if len(args) > 1 else []
            else:
                time_str = "30м"
        else:
            if len(args) >= 2:
                user_identifier = args[0]
                time_str = args[1]
                reason_parts = args[2:] if len(args) > 2 else []
            else:
                await message.reply("❌ Укажите пользователя и время.")
                return
        
        reason = " ".join(reason_parts) if reason_parts else "Не указана"
        
        duration_seconds, duration_text = parse_time_duration(time_str)
        
        if has_reply:
            user_id, user_display, found_users = await find_user_by_identifier(message, "")
        else:
            user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
        
        if not user_id and found_users:
            users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
            await message.reply(f"❓ Найдено несколько пользователей:\n\n{users_list}\n\nУточните запрос.")
            return
        elif not user_id:
            await message.reply("❌ Пользователь не найден.")
            return
        
        can_punish, error_message = await check_target_user(message, user_id)
        if not can_punish:
            await message.reply(error_message)
            return
        
        target_user = await bot.get_chat_member(message.chat.id, user_id)
        user_name = target_user.user.full_name
        warnings_count = await get_user_warnings(message.chat.id, user_id)
        
        try:
            permissions = {
                "can_send_messages": False,
                "can_send_media_messages": False,
                "can_send_polls": False,
                "can_send_other_messages": False,
                "can_add_web_page_previews": False,
                "can_change_info": False,
                "can_invite_users": False,
                "can_pin_messages": False
            }
            
            if duration_seconds:
                until_date = datetime.now() + timedelta(seconds=duration_seconds)
                await bot.restrict_chat_member(
                    chat_id=message.chat.id,
                    user_id=user_id,
                    permissions=permissions,
                    until_date=until_date
                )
            else:
                await bot.restrict_chat_member(
                    chat_id=message.chat.id,
                    user_id=user_id,
                    permissions=permissions
                )
            
            await add_user_warning(message.chat.id, user_id, message.from_user.id, f"Мут: {reason}")
            warnings_count = await get_user_warnings(message.chat.id, user_id)
            
            response = (
                f"✅ Пользователь заглушен\n\n"
                f"Пользователь: {user_name}\n"
                f"ID: {user_id}\n"
                f"Срок: {duration_text}\n"
                f"Причина: {reason}\n"
                f"Предупреждений: {warnings_count}\n"
            )
            
            if target_user.user.username:
                response += f"Username: @{target_user.user.username}\n"
            
            await message.reply(response, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Ошибка при муте: {e}")
            await message.reply("❌ Ошибка при выполнении команды.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /mute: {e}")
        await message.reply("❌ Произошла внутренняя ошибка.")

@dp.message(Command("unmute"))
async def cmd_unmute(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды модерации")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        user_identifier = None
        if has_reply:
            user_identifier = ""
        elif args:
            user_identifier = args[0]
        else:
            await message.reply("❌ Укажите пользователя или ответьте на сообщение.")
            return
        
        user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
        
        if not user_id and found_users:
            users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
            await message.reply(f"❓ Найдено несколько пользователей:\n\n{users_list}\n\nУточните запрос.")
            return
        elif not user_id:
            await message.reply("❌ Пользователь не найден.")
            return
        
        try:
            target_user = await bot.get_chat_member(message.chat.id, user_id)
            user_name = target_user.user.full_name
            
            permissions = {
                "can_send_messages": True,
                "can_send_media_messages": True,
                "can_send_polls": True,
                "can_send_other_messages": True,
                "can_add_web_page_previews": True,
                "can_change_info": False,
                "can_invite_users": False,
                "can_pin_messages": False
            }
            
            await bot.restrict_chat_member(
                chat_id=message.chat.id,
                user_id=user_id,
                permissions=permissions
            )
            
            response = (
                f"✅ Заглушка снята\n\n"
                f"Пользователь: {user_name}\n"
                f"ID: {user_id}\n"
            )
            
            if target_user.user.username:
                response += f"Username: @{target_user.user.username}\n"
            
            await message.reply(response, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Ошибка при снятии мута: {e}")
            await message.reply("❌ Произошла ошибка.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /unmute: {e}")
        await message.reply("❌ Произошла внутренняя ошибка.")

@dp.message(Command("ban"))
async def cmd_ban(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды модерации")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        if not args and not has_reply:
            help_text = (
                "⛔ Команда /ban\n\n"
                "Форматы использования:\n"
                "• /ban @username 7д причина\n"
                "• /ban 123456789 навсегда причина\n"
                "• /ban (ответ на сообщение) 30м причина\n"
            )
            await message.reply(help_text, parse_mode=ParseMode.HTML)
            return
        
        user_identifier = None
        time_str = None
        reason_parts = []
        
        if has_reply:
            if args:
                time_str = args[0]
                reason_parts = args[1:] if len(args) > 1 else []
            else:
                time_str = "30м"
        else:
            if len(args) >= 2:
                user_identifier = args[0]
                time_str = args[1]
                reason_parts = args[2:] if len(args) > 2 else []
            else:
                await message.reply("❌ Укажите пользователя и время.")
                return
        
        reason = " ".join(reason_parts) if reason_parts else "Не указана"
        
        duration_seconds, duration_text = parse_time_duration(time_str)
        
        if has_reply:
            user_id, user_display, found_users = await find_user_by_identifier(message, "")
        else:
            user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
        
        if not user_id and found_users:
            users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
            await message.reply(f"❓ Найдено несколько пользователей:\n\n{users_list}\n\nУточните запрос.")
            return
        elif not user_id:
            await message.reply("❌ Пользователь не найден.")
            return
        
        can_punish, error_message = await check_target_user(message, user_id)
        if not can_punish:
            await message.reply(error_message)
            return
        
        target_user = await bot.get_chat_member(message.chat.id, user_id)
        user_name = target_user.user.full_name
        warnings_count = await get_user_warnings(message.chat.id, user_id)
        
        try:
            if duration_seconds:
                until_date = datetime.now() + timedelta(seconds=duration_seconds)
                await bot.ban_chat_member(
                    chat_id=message.chat.id,
                    user_id=user_id,
                    until_date=until_date
                )
            else:
                await bot.ban_chat_member(
                    chat_id=message.chat.id,
                    user_id=user_id
                )
            
            await add_user_warning(message.chat.id, user_id, message.from_user.id, f"Бан: {reason}")
            warnings_count = await get_user_warnings(message.chat.id, user_id)
            
            response = (
                f"⛔ Пользователь забанен\n\n"
                f"Пользователь: {user_name}\n"
                f"ID: {user_id}\n"
                f"Срок: {duration_text}\n"
                f"Причина: {reason}\n"
                f"Предупреждений: {warnings_count}\n"
            )
            
            if target_user.user.username:
                response += f"Username: @{target_user.user.username}\n"
            
            await message.reply(response, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Ошибка при бане: {e}")
            await message.reply("❌ Произошла ошибка.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /ban: {e}")
        await message.reply("❌ Произошла внутренняя ошибка.")

@dp.message(Command("unban"))
async def cmd_unban(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды модерации")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        args = command.args.split() if command.args else []
        
        if not args:
            await message.reply("❌ Укажите ID пользователя.")
            return
        
        user_identifier = args[0]
        user_id = None
        
        if user_identifier.startswith('@'):
            username = user_identifier[1:].lower()
            chat_id_str = str(message.chat.id)
            if chat_id_str in stats["global"]:
                for uid_str, user_data in stats["global"][chat_id_str].items():
                    if user_data.get("username", "").lower() == username:
                        user_id = int(uid_str)
                        break
            
            if not user_id:
                await message.reply("❌ Не удалось найти пользователя. Используйте ID.")
                return
        else:
            try:
                user_id = int(user_identifier)
            except ValueError:
                await message.reply("❌ Неверный формат ID.")
                return
        
        try:
            await bot.unban_chat_member(
                chat_id=message.chat.id,
                user_id=user_id,
                only_if_banned=True
            )
            
            user_name = "Неизвестный"
            try:
                user = await bot.get_chat(user_id)
                user_name = user.full_name if hasattr(user, 'full_name') else user.first_name
            except:
                pass
            
            response = (
                f"✅ Пользователь разбанен\n\n"
                f"Пользователь: {user_name}\n"
                f"ID: {user_id}\n"
            )
            
            await message.reply(response, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Ошибка при разбане: {e}")
            await message.reply("❌ Произошла ошибка.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /unban: {e}")
        await message.reply("❌ Произошла внутренняя ошибка.")

@dp.message(Command("warns"))
async def cmd_warns(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        user_identifier = None
        if has_reply:
            user_identifier = ""
        elif args:
            user_identifier = args[0]
        else:
            await message.reply("❌ Укажите пользователя или ответьте на его сообщение.")
            return
        
        user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
        
        if not user_id and found_users:
            users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
            await message.reply(f"❓ Найдено несколько пользователей:\n\n{users_list}\n\nУточните запрос.")
            return
        elif not user_id:
            await message.reply("❌ Пользователь не найден.")
            return
        
        chat_id_str = str(message.chat.id)
        user_id_str = str(user_id)
        
        warnings = warnings_system[chat_id_str][user_id_str]
        
        if not warnings:
            await message.reply(f"✅ У пользователя нет предупреждений.")
            return
        
        try:
            target_user = await bot.get_chat_member(message.chat.id, user_id)
            user_name = target_user.user.full_name
        except:
            user_name = "Неизвестный"
        
        warnings_text = f"⚠ Предупреждения для {user_name}\n\n"
        
        for i, warning in enumerate(warnings, 1):
            date_str = datetime.fromisoformat(warning['date']).strftime("%d.%m.%Y %H:%M")
            warnings_text += (
                f"{i}. {warning['reason']}\n"
                f"   🕐 {date_str}\n"
                f"   👮 Админ ID: {warning['admin_id']}\n\n"
            )
        
        await message.reply(warnings_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /warns: {e}")
        await message.reply("❌ Произошла ошибка.")

@dp.message(Command("clearwarns"))
async def cmd_clearwarns(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        args = command.args.split() if command.args else []
        has_reply = message.reply_to_message is not None
        
        user_identifier = None
        if has_reply:
            user_identifier = ""
        elif args:
            user_identifier = args[0]
        else:
            await message.reply("❌ Укажите пользователя или ответьте на его сообщение.")
            return
        
        user_id, user_display, found_users = await find_user_by_identifier(message, user_identifier)
        
        if not user_id and found_users:
            users_list = "\n".join([f"{i+1}. {name}" for i, (uid, name) in enumerate(found_users[:5])])
            await message.reply(f"❓ Найдено несколько пользователей:\n\n{users_list}\n\nУточните запрос.")
            return
        elif not user_id:
            await message.reply("❌ Пользователь не найден.")
            return
        
        try:
            target_user = await bot.get_chat_member(message.chat.id, user_id)
            user_name = target_user.user.full_name
        except:
            user_name = "Неизвестный"
        
        await clear_user_warnings(message.chat.id, user_id)
        
        await message.reply(
            f"✅ Предупреждения для {user_name} очищены.",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Ошибка в команде /clearwarns: {e}")
        await message.reply("❌ Произошла ошибка.")

# ================== КОМАНДА ДЛЯ КИКА НОВЫХ УЧАСТНИКОВ ==================

@dp.message(Command("kicknew"))
async def cmd_kick_new(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды модерации")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        args = command.args.split() if command.args else []
        is_confirmed = "confirm" in args or "да" in args or "yes" in args
        
        hours = KICK_SETTINGS["default_hours"]
        for arg in args:
            try:
                hours = float(arg)
                if hours <= 0:
                    hours = KICK_SETTINGS["default_hours"]
                if hours > KICK_SETTINGS["max_hours"]:
                    await message.reply(f"❌ Максимальный период - {KICK_SETTINGS['max_hours']} часов.")
                    return
                break
            except ValueError:
                continue
        
        status_msg = await message.reply(
            f"🔍 Проверка новых участников...\n"
            f"📊 Период: {hours} час(ов)\n"
            f"⏳ Пожалуйста, подождите..."
        )
        
        recent_joiners = await get_recent_joiners(message.chat.id, hours)
        
        if not recent_joiners:
            await status_msg.edit_text(
                f"✅ За последние {hours} час(ов) не найдено новых участников.\n"
                f"📊 Все пользователи находятся в чате дольше указанного периода."
            )
            return
        
        joiner_list = "\n".join([
            f"{i+1}. {name} (ID: {user_id}) - вошел: {join_date.strftime('%d.%m.%Y %H:%M')}"
            for i, (user_id, name, join_date) in enumerate(recent_joiners[:20])
        ])
        
        if len(recent_joiners) > 20:
            joiner_list += f"\n... и еще {len(recent_joiners) - 20} пользователей"
        
        if not is_confirmed and KICK_SETTINGS["require_confirmation"]:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да, кикнуть всех", callback_data=f"kicknew_confirm_{hours}"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="kicknew_cancel")
                ]
            ])
            
            await status_msg.edit_text(
                f"⚠️ ВНИМАНИЕ! Обнаружено {len(recent_joiners)} новых участников за последние {hours} час(ов):\n\n"
                f"{joiner_list}\n\n"
                f"❓ Вы уверены, что хотите кикнуть всех этих пользователей?\n"
                f"⚡ Это действие нельзя отменить!",
                reply_markup=keyboard
            )
            return
        
        await status_msg.edit_text(
            f"🚫 Начинаю кик {len(recent_joiners)} пользователей...\n"
            f"⏳ Пожалуйста, подождите, это может занять некоторое время."
        )
        
        kicked_count = 0
        failed_count = 0
        failed_users = []
        
        for user_id, user_name, join_date in recent_joiners:
            try:
                chat_member = await bot.get_chat_member(message.chat.id, user_id)
                if chat_member.status in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
                    failed_count += 1
                    failed_users.append(f"{user_name} (администратор)")
                    continue
                
                await bot.ban_chat_member(message.chat.id, user_id)
                await bot.unban_chat_member(message.chat.id, user_id, only_if_banned=True)
                kicked_count += 1
                
                await add_user_warning(
                    message.chat.id, 
                    user_id, 
                    message.from_user.id, 
                    f"Кик как новый участник (в течение {hours} часов)"
                )
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Ошибка при кике пользователя {user_id}: {e}")
                failed_count += 1
                failed_users.append(f"{user_name} (ошибка: {str(e)[:50]})")
        
        result_text = (
            f"✅ Операция завершена!\n\n"
            f"📊 Статистика:\n"
            f"• Кикнуто: {kicked_count} пользователей\n"
            f"• Ошибок: {failed_count}\n"
            f"• Всего найдено: {len(recent_joiners)}\n\n"
            f"📅 Период: последние {hours} час(ов)\n"
            f"👤 Админ: {message.from_user.full_name}"
        )
        
        if failed_users and len(failed_users) <= 10:
            result_text += f"\n\n❌ Не удалось кикнуть:\n" + "\n".join(failed_users[:10])
        
        await status_msg.edit_text(result_text)
        
        logger.warning(
            f"Админ {message.from_user.id} кикнул {kicked_count} новых участников "
            f"в чате {message.chat.id} за последние {hours} часов"
        )
        
    except Exception as e:
        logger.error(f"Ошибка в команде /kicknew: {e}")
        await message.reply(f"❌ Произошла ошибка: {str(e)[:100]}")

@dp.callback_query(lambda c: c.data and c.data.startswith("kicknew_"))
async def handle_kicknew_callback(callback: CallbackQuery):
    try:
        if callback.data == "kicknew_cancel":
            await callback.message.edit_text("❌ Операция отменена.")
            await callback.answer("Отменено")
            return
        
        if callback.data.startswith("kicknew_confirm_"):
            hours = float(callback.data.split("_")[2])
            
            chat_member = await bot.get_chat_member(callback.message.chat.id, callback.from_user.id)
            if chat_member.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
                await callback.answer("❌ У вас нет прав для подтверждения!", show_alert=True)
                return
            
            class FakeMessage:
                def __init__(self, chat, from_user):
                    self.chat = chat
                    self.from_user = from_user
                    self.reply = callback.message.reply
            
            class FakeCommand:
                args = [str(hours), "confirm"]
            
            fake_message = FakeMessage(callback.message.chat, callback.from_user)
            fake_command = FakeCommand()
            
            await callback.message.edit_text("🔄 Подтверждено, выполняю кик...")
            await cmd_kick_new(fake_message, fake_command)
            await callback.answer()
            
    except Exception as e:
        logger.error(f"Ошибка в callback kicknew: {e}")
        await callback.answer(f"Ошибка: {str(e)[:50]}", show_alert=True)

@dp.message(Command("listnew"))
async def cmd_list_new(message: Message, command: CommandObject):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        args = command.args.split() if command.args else []
        hours = KICK_SETTINGS["default_hours"]
        
        if args:
            try:
                hours = float(args[0])
            except:
                pass
        
        recent_joiners = await get_recent_joiners(message.chat.id, hours)
        
        if not recent_joiners:
            await message.reply(f"✅ За последние {hours} час(ов) нет новых участников.")
            return
        
        joiner_list = "\n".join([
            f"• {name} (ID: {user_id}) - {join_date.strftime('%d.%m.%Y %H:%M')}"
            for user_id, name, join_date in recent_joiners[:30]
        ])
        
        text = (
            f"📊 Новые участники за последние {hours} час(ов):\n\n"
            f"{joiner_list}\n\n"
            f"Всего: {len(recent_joiners)} пользователей\n"
            f"💡 Используйте /kicknew {hours} confirm чтобы кикнуть их"
        )
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Ошибка в /listnew: {e}")
        await message.reply("❌ Произошла ошибка")

@dp.message(Command("kicknew_clear"))
async def cmd_clear_join_events(message: Message):
    try:
        if not await check_admin_permissions(message):
            await message.reply("❌ У вас нет прав для использования этой команды.")
            return
        
        if os.path.exists(JOIN_EVENTS_FILE):
            os.remove(JOIN_EVENTS_FILE)
            await message.reply("✅ История событий входа очищена.")
        else:
            await message.reply("📭 Файл с историей не найден.")
            
    except Exception as e:
        logger.error(f"Ошибка в /kicknew_clear: {e}")
        await message.reply("❌ Произошла ошибка")

# ================== ИГРОВЫЕ КОМАНДЫ ==================

@dp.message(Command("додеп"))
async def cmd_dodep(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "games")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /додеп")
            return
        
        update_cooldown(message.from_user.id, "games")
        await message.answer_dice(emoji="🎲")
    except Exception as e:
        logger.error(f"Ошибка в команде /додеп: {e}")
        await message.reply("🎲")

@dp.message(Command("future"))
async def cmd_future(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "future")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /future")
            return
        
        update_cooldown(message.from_user.id, "future")
        
        prediction = random.choice(FUTURE_PREDICTIONS)
        user_name = message.from_user.first_name or "Пользователь"
        if message.from_user.last_name:
            user_name += f" {message.from_user.last_name}"
        
        future_text = (
            f"🔮 Предсказание для {user_name}\n\n"
            f"{prediction}\n\n"
            f"________________"
        )
        
        await message.answer(future_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /future: {e}")
        await message.reply("🔮 Что-то пошло не так... Попробуйте позже.")

# ================== КОМАНДЫ ДЛЯ РАЗРЕШЕННЫХ ID ==================

@dp.message(Command("say"))
async def cmd_say(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для отправки сообщений от имени бота.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        if not command.args:
            await message.reply("❌ Формат: /say текст_сообщения")
            return
        
        await message.answer(command.args, parse_mode=ParseMode.HTML)
        
        try:
            await message.delete()
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка в команде /say: {e}")

@dp.message(Command("announce"))
async def cmd_announce(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для отправки объявлений от имени бота.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        if not command.args:
            await message.reply("📢 Формат: /announce текст_объявления")
            return
        
        await message.answer(command.args, parse_mode=ParseMode.HTML)
        
        try:
            await message.delete()
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка в команде /announce: {e}")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject):
    try:
        if not is_allowed_bot_sender(message.from_user.id):
            await message.reply("❌ У вас нет прав для рассылки сообщений.")
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "moderation")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды")
            return
        
        update_cooldown(message.from_user.id, "moderation")
        
        if not command.args:
            await message.reply("❌ Укажите текст для рассылки.")
            return
        
        all_chats = list(stats["global"].keys())
        
        if not all_chats:
            await message.reply("❌ Бот еще не добавлен ни в один чат.")
            return
        
        success_count = 0
        fail_count = 0
        
        for chat_id in all_chats:
            try:
                await bot.send_message(
                    chat_id=int(chat_id),
                    text=command.args,
                    parse_mode=ParseMode.HTML
                )
                success_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Не удалось отправить в чат {chat_id}: {e}")
                fail_count += 1
        
        report = (
            f"📊 Рассылка завершена\n\n"
            f"✅ Успешно: {success_count} чатов\n"
            f"❌ Ошибок: {fail_count} чатов"
        )
        
        await message.reply(report, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /broadcast: {e}")

# ================== ОСНОВНЫЕ КОМАНДЫ ==================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "start")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /start")
            return
        
        update_cooldown(message.from_user.id, "start")
        
        welcome_text = (
            "👋 Добро пожаловать в бот!\n\n"
            "📈 Я считаю активность участников в групповых чатах:\n"
            "• Подсчитываю все сообщения\n"
            "• Веду дневную и общую статистику\n"
            "• Автоматически удаляю запрещенные слова\n\n"
            "🎮 Игровые команды:\n"
            "/додеп - кинуть кубик 🎲\n"
            "/future - получить предсказание 🔮\n"
            "/duel - вызвать на дуэль 🔫\n\n"
            "👤 Профиль и достижения:\n"
            "/profile - ваш профиль\n"
            "/achievements - ваши достижения\n"
            "/list_ach - список всех достижений\n\n"
            "📊 Статистика:\n"
            "/top - Топ активности\n"
            "/stats - Общая статистика\n\n"
            "📋 Правила:\n"
            "/rules - Правила чата\n\n"
            "📱 Используйте кнопки ниже для навигации"
        )
        
        await message.answer(welcome_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в команде /start: {e}")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    try:
        can_use, remaining = check_cooldown(message.from_user.id, "help")
        if not can_use:
            await send_cooldown_message(message, remaining, "команды /help")
            return
        
        update_cooldown(message.from_user.id, "help")
        
        help_text = (
            "💬 Ссылки на источники\n"
            "ТГК - https://t.me/opg_media\n\n"
            "Создатель бота @TEMHbIU_PRINTS\n\n"
            "📋 Все команды бота:\n"
            "/start - Главное меню\n"
            "/profile - Профиль\n"
            "/achievements - Достижения\n"
            "/list_ach - Список достижений\n"
            "/top - Топ активности\n"
            "/stats - Статистика\n"
            "/rules - Правила\n"
            "/додеп - Кинуть кубик\n"
            "/future - Предсказание\n"
            "/duel - Дуэль 🔫\n"
        )
        await message.answer(help_text, parse_mode=ParseMode.HTML, reply_markup=get_help_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в команде /help: {e}")

# ================== ЗАПРЕЩЕННЫЕ СЛОВА ==================

def normalize_text(text: str) -> str:
    text = text.lower()
    
    replacements = {
        'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р',
        'y': 'у', 'x': 'х', '0': 'о', '1': 'і', '3': 'з',
        '6': 'б', '@': 'а', '$': 'с', '&': 'и'
    }
    
    for eng, rus in replacements.items():
        text = text.replace(eng, rus)
    
    return text

def contains_banned_words(text: str) -> Tuple[bool, List[str]]:
    found_words = []
    
    if not text:
        return False, found_words
    
    normalized = normalize_text(text)
    
    for word in BANNED_WORDS:
        if word in normalized:
            found_words.append(word)
    
    found_words = list(set(found_words))
    return len(found_words) > 0, found_words

async def handle_banned_words(message: Message, found_words: List[str]):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        try:
            await message.delete()
        except TelegramBadRequest as e:
            logger.error(f"Не удалось удалить сообщение: {e}")
        
        chat_id_str = str(chat_id)
        user_id_str = str(user_id)
        
        if chat_id_str not in violators:
            violators[chat_id_str] = {}
        
        if user_id_str not in violators[chat_id_str]:
            violators[chat_id_str][user_id_str] = {
                "violations": 1,
                "last_violation": datetime.now().timestamp(),
                "name": message.from_user.first_name or "Неизвестный"
            }
        else:
            violators[chat_id_str][user_id_str]["violations"] += 1
            violators[chat_id_str][user_id_str]["last_violation"] = datetime.now().timestamp()
        
        await add_user_warning(chat_id, user_id, 0, f"Запрещенные слова: {', '.join(found_words[:3])}")
        warnings_count = await get_user_warnings(chat_id, user_id)
        
        warning_text = (
            f"⚠ Нарушение правил!\n\n"
            f"Пользователь: {message.from_user.first_name or 'Неизвестный'}\n"
            f"Обнаружены запрещенные слова: {', '.join(found_words[:3])}\n"
        )
        
        violations_count = violators[chat_id_str][user_id_str]["violations"]
        warning_text += f"Нарушений: {violations_count}\n"
        warning_text += f"Предупреждений: {warnings_count}\n\n"
        
        if violations_count >= 3:
            try:
                until_date = datetime.now() + timedelta(hours=24)
                await bot.restrict_chat_member(
                    chat_id=chat_id,
                    user_id=user_id,
                    permissions={
                        "can_send_messages": False,
                        "can_send_media_messages": False,
                        "can_send_polls": False,
                        "can_send_other_messages": False,
                        "can_add_web_page_previews": False,
                        "can_change_info": False,
                        "can_invite_users": False,
                        "can_pin_messages": False
                    },
                    until_date=until_date
                )
                warning_text += f"⏰ Автоматический мут на 24 часа (3+ нарушения)"
            except Exception as e:
                logger.error(f"Ошибка при автоматическом муте: {e}")
                warning_text += "❌ Не удалось применить наказание"
        elif violations_count == 2:
            warning_text += "⚠ Последнее предупреждение! Следующее нарушение - мут на 24 часа."
        else:
            warning_text += "📝 Первое предупреждение. Будьте внимательнее!"
        
        warning_msg = await message.answer(warning_text, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(10)
        try:
            await warning_msg.delete()
        except:
            pass
        
        logger.info(f"Удалено сообщение с запрещенными словами от {user_id}: {found_words}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке запрещенных слов: {e}")

# ================== ОБРАБОТЧИК НОВЫХ УЧАСТНИКОВ ==================

@dp.message(F.new_chat_members)
async def handle_new_members(message: Message):
    try:
        for new_member in message.new_chat_members:
            if not new_member.is_bot:
                await save_join_event(message.chat.id, new_member.id, new_member.full_name)
                
                chat_id_str = str(message.chat.id)
                user_id_str = str(new_member.id)
                
                if chat_id_str not in stats["global"]:
                    stats["global"][chat_id_str] = {}
                
                if user_id_str not in stats["global"][chat_id_str]:
                    stats["global"][chat_id_str][user_id_str] = {
                        "name": new_member.full_name,
                        "messages": 0,
                        "first_seen": datetime.now().isoformat()
                    }
                    save_data()
                    
                logger.info(f"Новый участник в чате {message.chat.id}: {new_member.full_name} ({new_member.id})")
                
    except Exception as e:
        logger.error(f"Ошибка при обработке нового участника: {e}")

# ================== ОБРАБОТЧИК СООБЩЕНИЙ ==================

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def handle_group_message(message: Message):
    try:
        if not message.from_user:
            return
        
        if message.text and message.text.startswith('/'):
            return
        
        can_use, remaining = check_cooldown(message.from_user.id, "any_message")
        if not can_use:
            return
        
        update_cooldown(message.from_user.id, "any_message")
        
        text = message.text or message.caption or ""
        
        if text:
            has_banned, found_words = contains_banned_words(text)
            
            if has_banned:
                await handle_banned_words(message, found_words)
                return
        
        user_id = message.from_user.id
        user_name = message.from_user.first_name or "Неизвестный"
        
        if message.from_user.last_name:
            user_name += f" {message.from_user.last_name}"
        
        update_user_stats(message.chat.id, user_id, user_name)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")

# ================== ПЕРИОДИЧЕСКИЕ ЗАДАЧИ ==================

async def cleanup_old_stats():
    try:
        cutoff_date = date.today() - timedelta(days=30)
        cutoff_str = str(cutoff_date)
        
        for chat_id in list(stats["daily"].keys()):
            for day in list(stats["daily"][chat_id].keys()):
                if day < cutoff_str:
                    del stats["daily"][chat_id][day]
            
            if not stats["daily"][chat_id]:
                del stats["daily"][chat_id]
        
        current_time = datetime.now().timestamp()
        for chat_id in list(violators.keys()):
            for user_id in list(violators[chat_id].keys()):
                if violators[chat_id][user_id]["last_violation"] < current_time - 604800:
                    del violators[chat_id][user_id]
            
            if not violators[chat_id]:
                del violators[chat_id]
        
        save_data()
        logger.info("Старые данные очищены")
        
    except Exception as e:
        logger.error(f"Ошибка при очистке старых данных: {e}")

async def cleanup_old_cooldowns():
    try:
        cutoff_time = datetime.now().timestamp() - 3600
        users_to_remove = []
        
        for user_id, cooldowns in user_cooldowns.items():
            for key in list(cooldowns.keys()):
                if cooldowns[key] < cutoff_time:
                    del cooldowns[key]
            
            if not cooldowns:
                users_to_remove.append(user_id)
        
        for user_id in users_to_remove:
            del user_cooldowns[user_id]
        
        logger.debug(f"Очищены CD записи, осталось пользователей: {len(user_cooldowns)}")
    except Exception as e:
        logger.error(f"Ошибка при очистке CD: {e}")

async def create_backup():
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            
        backup_file = os.path.join(BACKUP_DIR, f"backup_{date.today()}.json")
        with open(backup_file, 'w', encoding='utf-8') as f:
            backup_data = {
                "stats": stats,
                "violators": dict(violators),
                "warnings_system": dict(warnings_system)
            }
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Резервная копия создана: {backup_file}")
        
        for file in os.listdir(BACKUP_DIR):
            if file.startswith('backup_') and file.endswith('.json'):
                file_path = os.path.join(BACKUP_DIR, file)
                file_time = os.path.getctime(file_path)
                if datetime.now().timestamp() - file_time > 7 * 24 * 3600:
                    os.remove(file_path)
                    logger.info(f"Удалена старая резервная копия: {file}")
    except Exception as e:
        logger.error(f"Ошибка при создании резервной копии: {e}")

async def periodic_tasks():
    while True:
        await asyncio.sleep(3600)
        
        if datetime.now().hour == 3:
            await cleanup_old_stats()
        
        if datetime.now().hour == 2:
            await create_backup()
        
        await cleanup_old_cooldowns()

async def check_telegram_connection():
    try:
        await bot.get_me()
        return True
    except Exception as e:
        logger.error(f"Потеря соединения с Telegram: {e}")
        return False

async def connection_monitor():
    while True:
        await asyncio.sleep(60)
        if not await check_telegram_connection():
            logger.warning("Проблемы с соединением с Telegram")

# ================== CALLBACK ХЕНДЛЕРЫ ==================

@dp.callback_query(F.data == "my_profile")
async def handle_my_profile_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        profile = get_user_profile(
            callback.message.chat.id,
            callback.from_user.id,
            callback.from_user.full_name
        )
        
        chat = await bot.get_chat(callback.message.chat.id)
        chat_title = chat.title or "чате"
        
        achievements = achievement_system.get_user_achievements(callback.message.chat.id, callback.from_user.id)
        duel_stats = achievement_system.get_duel_stats(callback.from_user.id)
        
        profile_text = format_profile(profile, chat_title, achievements)
        profile_text += f"\n🔫 Дуэли: {duel_stats['wins']} побед / {duel_stats['losses']} поражений"
        
        await callback.message.edit_text(profile_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback my_profile: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "my_achievements")
async def handle_my_achievements_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        achievements = achievement_system.get_user_achievements(callback.message.chat.id, callback.from_user.id)
        duel_stats = achievement_system.get_duel_stats(callback.from_user.id)
        
        if not achievements and duel_stats['wins'] == 0 and duel_stats['losses'] == 0:
            text = "📭 У вас пока нет достижений."
        else:
            text = f"🏆 Ваши достижения\n\n"
            text += f"🔫 Статистика дуэлей: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n"
            text += achievement_system.format_achievements(achievements)
        
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback my_achievements: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("top_"))
async def handle_top_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        parts = callback.data.split("_")
        if len(parts) < 2:
            await callback.answer("❌ Неверный формат запроса", show_alert=True)
            return
            
        period = parts[1]
        chat_id = callback.message.chat.id
        
        top_users = get_top_users(chat_id, period, 10)
        text = format_top_message(chat_id, period, top_users)
        
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback top: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "rules")
async def handle_rules_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        chat_id = str(callback.message.chat.id)
        rules_text = chat_rules.get(chat_id, DEFAULT_RULES)
        
        if is_allowed_bot_sender(callback.from_user.id):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✏️ Редактировать", callback_data="edit_rules"),
                    InlineKeyboardButton(text="📊 К статистике", callback_data="back_to_stats")
                ]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="👤 Профиль", callback_data="my_profile"),
                    InlineKeyboardButton(text="📊 К статистике", callback_data="back_to_stats")
                ]
            ])
        
        await callback.message.edit_text(rules_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback rules: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "edit_rules")
async def handle_edit_rules_callback(callback: CallbackQuery):
    try:
        if not is_allowed_bot_sender(callback.from_user.id):
            await callback.answer("❌ У вас нет прав", show_alert=True)
            return
        
        help_text = (
            "✏️ Редактирование правил\n\n"
            "Используйте команды в чате:\n\n"
            "/rules set [текст] - новые правила\n"
            "/rules add [текст] - добавить пункт\n"
            "/rules remove [номер] - удалить пункт\n"
            "/rules reset - сбросить\n\n"
            "Поддерживается HTML разметка"
        )
        
        await callback.message.edit_text(help_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback edit_rules: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "help")
async def handle_help_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        help_text = (
            "💬 Ссылки на источники\n"
            "ТГК - https://t.me/opg_media\n\n"
            "Создатель бота @TEMHbIU_PRINTS"
        )
        
        await callback.message.edit_text(help_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback help: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(F.data == "back_to_stats")
async def handle_back_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        chat_id = callback.message.chat.id
        top_today = get_top_users(chat_id, "today", 10)
        
        if top_today:
            text = format_top_message(chat_id, "today", top_today)
        else:
            text = "📭 Статистика за сегодня пока пуста."
        
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при возврате к статистике: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith("ach_") and c.data.count("_") == 1)
async def handle_user_achievements_callback(callback: CallbackQuery):
    try:
        can_use, remaining = check_cooldown(callback.from_user.id, "callback")
        if not can_use:
            await callback.answer(f"⏳ Подождите {remaining} сек.", show_alert=False)
            return
        
        update_cooldown(callback.from_user.id, "callback")
        
        try:
            user_id = int(callback.data.split("_")[1])
        except (ValueError, IndexError):
            await callback.answer("❌ Неверный ID пользователя", show_alert=True)
            return
        
        try:
            user = await bot.get_chat_member(callback.message.chat.id, user_id)
            user_name = user.user.full_name
        except Exception as e:
            logger.error(f"Не удалось получить информацию о пользователе {user_id}: {e}")
            user_name = f"Пользователь (ID: {user_id})"
        
        achievements = achievement_system.get_user_achievements(callback.message.chat.id, user_id)
        duel_stats = achievement_system.get_duel_stats(user_id)
        
        if not achievements and duel_stats['wins'] == 0 and duel_stats['losses'] == 0:
            text = f"📭 У пользователя {user_name} пока нет достижений."
        else:
            text = f"🏆 Достижения пользователя {user_name}\n\n"
            text += f"🔫 Статистика дуэлей: {duel_stats['wins']} побед / {duel_stats['losses']} поражений\n\n"
            text += achievement_system.format_achievements(achievements)
        
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в callback user achievements: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

# ================== ЗАПУСК ==================

async def main():
    """Главная функция"""
    load_data()
    
    # Создаем директории для данных если их нет
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(STATS_FILE) if os.path.dirname(STATS_FILE) else 'data', exist_ok=True)
    
    # Запускаем периодические задачи
    asyncio.create_task(periodic_tasks())
    asyncio.create_task(connection_monitor())
    
    logger.info("=" * 50)
    logger.info("🤖 Telegram Bot запущен")
    logger.info(f"📅 Дата запуска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"📊 Загружено чатов: {len(stats['global'])}")
    logger.info("📌 Команда /kicknew - кик новых участников")
    logger.info("=" * 50)
    
    # Удаляем вебхук и запускаем polling
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
