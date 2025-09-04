import asyncpg
import logging
from asyncpg import Connection, Pool, create_pool
from contextlib import asynccontextmanager
from typing import List, Dict, Optional, AsyncIterator
from datetime import datetime

from config import DB_CONFIG
from exceptions import DatabaseError

logger = logging.getLogger(__name__)

# Константы таблиц
TAB_ACC = 'accounts'
TAB_CHAN = 'channels'
TAB_LANG = 'languages'
TAB_STAT = 'statistics'

# Глобальный пул соединений
_db_pool: Pool = None

async def init_db_pool():
    """Инициализация пула соединений"""
    global _db_pool
    try:
        _db_pool = await create_pool(
            **DB_CONFIG,
            min_size=5,
            max_size=20,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            timeout=30
        )
        logger.info("Database pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise DatabaseError(f"Database pool initialization failed: {e}")

async def get_db_connection() -> Connection:
    """Получение соединения из пула"""
    if _db_pool is None:
        raise DatabaseError("Database pool not initialized")
    
    try:
        return await _db_pool.acquire()
    except Exception as e:
        logger.error(f"Failed to acquire database connection: {e}")
        raise DatabaseError(f"Failed to get database connection: {e}")

async def close_db_connection(conn: Connection):
    """Возврат соединения в пул"""
    if _db_pool is not None and conn is not None:
        try:
            if not conn.is_closed():
                await _db_pool.release(conn)
        except Exception as e:
            logger.error(f"Error releasing connection: {e}")

async def shutdown_db_pool():
    """Закрытие пула соединений"""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
        logger.info("Database pool closed")

@asynccontextmanager
async def db_session() -> AsyncIterator[Connection]:
    """Контекстный менеджер для работы с БД"""
    conn = await get_db_connection()
    try:
        yield conn
    finally:
        await close_db_connection(conn)

async def create_tables():
    """Создание/обновление таблиц в БД"""
    async with db_session() as conn:
        try:
            async with conn.transaction():
                # Таблица языков
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TAB_LANG}(
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Таблица каналов
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TAB_CHAN}(
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        lang TEXT REFERENCES {TAB_LANG}(name) ON DELETE CASCADE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(name, lang)
                    )
                """)

                # Таблица аккаунтов (с полем fail)
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TAB_ACC}(
                        id SERIAL PRIMARY KEY,
                        phone_number TEXT NOT NULL UNIQUE,
                        session_data TEXT NOT NULL,
                        lang TEXT DEFAULT 'none',
                        status TEXT DEFAULT 'active' CHECK (status IN ('active', 'pause', 'ban')),
                        fail INTEGER DEFAULT 0,
                        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Таблица статистики (базовая - убрали лишние поля)
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TAB_STAT}(
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL,
                        views INTEGER DEFAULT 0,
                        subs INTEGER DEFAULT 0,
                        lang TEXT DEFAULT 'all',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Добавляем поле fail если его нет (для совместимости)
                try:
                    await conn.execute(f"ALTER TABLE {TAB_ACC} ADD COLUMN IF NOT EXISTS fail INTEGER DEFAULT 0")
                except:
                    pass  # Поле уже существует

                # Индексы для оптимизации
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_lang ON {TAB_ACC}(lang)")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_status ON {TAB_ACC}(status)")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_phone ON {TAB_ACC}(phone_number)")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_chan_lang ON {TAB_CHAN}(lang)")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_stat_date ON {TAB_STAT}(date)")
                
                # Индексы для забаненных аккаунтов
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_ban_retry ON {TAB_ACC}(status, last_used) WHERE status = 'ban'")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_updated_status ON {TAB_ACC}(updated_at, status)")
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_acc_ban_24h ON {TAB_ACC}(updated_at) WHERE status = 'ban'")

                # Триггер для обновления updated_at
                await conn.execute(f"""
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql';
                """)

                await conn.execute(f"""
                    DROP TRIGGER IF EXISTS update_{TAB_ACC}_updated_at ON {TAB_ACC};
                    CREATE TRIGGER update_{TAB_ACC}_updated_at
                        BEFORE UPDATE ON {TAB_ACC}
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_at_column();
                """)

            logger.info("Database tables created successfully with simplified statistics")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise DatabaseError(f"Table creation failed: {e}")

# === Account Management ===

async def add_account(phone: str, session_data: str, lang: str = 'none') -> bool:
    """Добавляет аккаунт в БД"""
    async with db_session() as conn:
        try:
            await conn.execute(
                f"""INSERT INTO {TAB_ACC} (phone_number, session_data, lang) 
                   VALUES ($1, $2, $3)
                   ON CONFLICT (phone_number) DO NOTHING""",
                phone, session_data, lang
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add account {phone}: {e}")
            return False

async def get_account_by_phone(phone: str) -> Optional[Dict]:
    """Получает аккаунт по номеру телефона"""
    async with db_session() as conn:
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {TAB_ACC} WHERE phone_number = $1", phone
            )
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Failed to get account {phone}: {e}")
            return None

async def get_accounts_by_lang(lang: str, status: str = None) -> List[Dict]:
    """Получает аккаунты по языку и статусу"""
    async with db_session() as conn:
        try:
            if status:
                query = f"SELECT * FROM {TAB_ACC} WHERE lang = $1 AND status = $2 ORDER BY id"
                rows = await conn.fetch(query, lang, status)
            else:
                query = f"SELECT * FROM {TAB_ACC} WHERE lang = $1 ORDER BY id"
                rows = await conn.fetch(query, lang)
            
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get accounts for lang {lang}: {e}")
            return []

async def get_all_accounts(status: str = None) -> List[Dict]:
    """Получает все аккаунты"""
    async with db_session() as conn:
        try:
            if status:
                query = f"SELECT * FROM {TAB_ACC} WHERE status = $1 ORDER BY id"
                rows = await conn.fetch(query, status)  
            else:
                query = f"SELECT * FROM {TAB_ACC} ORDER BY id"
                rows = await conn.fetch(query)
            
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get all accounts: {e}")
            return []

async def update_account_status(phone: str, status: str) -> bool:
    """Обновляет статус аккаунта"""
    async with db_session() as conn:
        try:
            result = await conn.execute(
                f"UPDATE {TAB_ACC} SET status = $1 WHERE phone_number = $2",
                status, phone
            )
            return "UPDATE 1" in result
        except Exception as e:
            logger.error(f"Failed to update account {phone} status: {e}")
            return False

async def increment_account_fails(phone: str) -> int:
    """Увеличивает счетчик неудач и возвращает новое значение"""
    async with db_session() as conn:
        try:
            result = await conn.fetchrow(
                f"UPDATE {TAB_ACC} SET fail = fail + 1, last_used = CURRENT_TIMESTAMP WHERE phone_number = $1 RETURNING fail",
                phone
            )
            return result['fail'] if result else 0
        except Exception as e:
            logger.error(f"Failed to increment fails for {phone}: {e}")
            return 0

async def reset_account_fails(phone: str) -> bool:
    """Сбрасывает счетчик неудач и возвращает в active"""
    async with db_session() as conn:
        try:
            await conn.execute(
                f"UPDATE {TAB_ACC} SET fail = 0, status = 'active', last_used = CURRENT_TIMESTAMP WHERE phone_number = $1",
                phone
            )
            return True
        except Exception as e:
            logger.error(f"Failed to reset fails for {phone}: {e}")
            return False

async def get_ban_accounts_for_retry() -> List[Dict]:
    """Получает забаненные аккаунты, готовые для повторной проверки (раз в 120 часов)"""
    async with db_session() as conn:
        try:
            # Аккаунты со статусом 'ban', которые не проверялись 120 часов
            query = f"""
                SELECT * FROM {TAB_ACC} 
                WHERE status = 'ban' 
                AND (last_used IS NULL OR last_used < NOW() - INTERVAL '120 hours')
                ORDER BY last_used ASC NULLS FIRST
                LIMIT 100
            """
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get ban accounts for retry: {e}")
            return []

async def mark_account_retry_attempt(phone: str):
    """Отмечает попытку проверки забаненного аккаунта"""
    async with db_session() as conn:
        try:
            await conn.execute(
                f"UPDATE {TAB_ACC} SET last_used = CURRENT_TIMESTAMP WHERE phone_number = $1",
                phone
            )
        except Exception as e:
            logger.error(f"Failed to mark retry attempt for {phone}: {e}")

async def delete_accounts_by_status(status: str, limit: int = None) -> int:
    """Удаляет аккаунты по статусу"""
    async with db_session() as conn:
        try:
            if limit:
                query = f"DELETE FROM {TAB_ACC} WHERE status = $1 AND id IN (SELECT id FROM {TAB_ACC} WHERE status = $1 LIMIT $2)"
                result = await conn.execute(query, status, limit)
            else:
                query = f"DELETE FROM {TAB_ACC} WHERE status = $1"
                result = await conn.execute(query, status)
            
            # Парсим результат DELETE
            deleted_count = int(result.split()[-1]) if result.startswith('DELETE') else 0
            logger.info(f"Deleted {deleted_count} accounts with status {status}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to delete accounts with status {status}: {e}")
            return 0

# === УПРОЩЕННАЯ СТАТИСТИКА АККАУНТОВ ===

async def get_account_stats() -> Dict[str, int]:
    """Получает упрощенную статистику аккаунтов"""
    async with db_session() as conn:
        try:
            stats = {}
            
            # Общее количество
            stats['total'] = await conn.fetchval(f"SELECT COUNT(*) FROM {TAB_ACC}")
            
            # По статусам
            for status in ['active', 'pause', 'ban']:
                stats[status] = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {TAB_ACC} WHERE status = $1", status
                )
            
            # По языкам (оставляем для статистики по языкам)
            lang_stats = await conn.fetch(
                f"SELECT lang, COUNT(*) as count FROM {TAB_ACC} GROUP BY lang"
            )
            stats['by_language'] = {row['lang']: row['count'] for row in lang_stats}
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get account stats: {e}")
            return {}

async def get_banned_accounts_24h() -> int:
    """Получает количество забаненных аккаунтов за последние 24 часа"""
    async with db_session() as conn:
        try:
            result = await conn.fetchval(
                f"""SELECT COUNT(*) FROM {TAB_ACC} 
                   WHERE status = 'ban' 
                   AND updated_at >= NOW() - INTERVAL '24 hours'"""
            )
            return result or 0
        except Exception as e:
            logger.error(f"Failed to get banned accounts 24h stats: {e}")
            return 0

# === Channel Management ===

async def add_channel(name: str, lang: str) -> bool:
    """Добавляет канал"""
    async with db_session() as conn:
        try:
            await conn.execute(
                f"INSERT INTO {TAB_CHAN} (name, lang) VALUES ($1, $2)",
                name, lang
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add channel {name}: {e}")
            return False

async def delete_channel(name: str, lang: str) -> bool:
    """Удаляет канал"""
    async with db_session() as conn:
        try:
            result = await conn.execute(
                f"DELETE FROM {TAB_CHAN} WHERE name = $1 AND lang = $2",
                name, lang
            )
            return "DELETE 1" in result
        except Exception as e:
            logger.error(f"Failed to delete channel {name}: {e}")
            return False

async def get_channels_by_lang(lang: str) -> List[str]:
    """Получает каналы по языку"""
    async with db_session() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT name FROM {TAB_CHAN} WHERE lang = $1", lang
            )
            return [row['name'] for row in rows]
        except Exception as e:
            logger.error(f"Failed to get channels for lang {lang}: {e}")
            return []

# === Language Management ===

async def add_language(name: str) -> bool:
    """Добавляет язык"""
    async with db_session() as conn:
        try:
            await conn.execute(
                f"INSERT INTO {TAB_LANG} (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                name
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add language {name}: {e}")
            return False

async def get_all_languages() -> List[str]:
    """Получает все языки"""
    async with db_session() as conn:
        try:
            rows = await conn.fetch(f"SELECT name FROM {TAB_LANG} ORDER BY name")
            return [row['name'] for row in rows]
        except Exception as e:
            logger.error(f"Failed to get languages: {e}")
            return []

# === БАЗОВАЯ СТАТИСТИКА ЗАДАЧ (убрали сложные метрики) ===

async def record_task_execution(task_type: str, success: bool, lang: str = 'all'):
    """Записывает базовую статистику выполнения задач"""
    async with db_session() as conn:
        try:
            today = datetime.now().date()
            
            # Обновляем или создаем запись статистики
            if success:
                if task_type == 'view':
                    await conn.execute(
                        f"""INSERT INTO {TAB_STAT} (date, views, lang) VALUES ($1, 1, $2)
                           ON CONFLICT (date, lang) DO UPDATE SET 
                           views = {TAB_STAT}.views + 1""",
                        today, lang
                    )
                elif task_type == 'subscribe':
                    await conn.execute(
                        f"""INSERT INTO {TAB_STAT} (date, subs, lang) VALUES ($1, 1, $2)
                           ON CONFLICT (date, lang) DO UPDATE SET 
                           subs = {TAB_STAT}.subs + 1""",
                        today, lang
                    )
                    
        except Exception as e:
            logger.error(f"Failed to record task execution: {e}")

async def get_daily_statistics(days: int = 7) -> List[Dict]:
    """Получает дневную статистику за указанное количество дней"""
    async with db_session() as conn:
        try:
            rows = await conn.fetch(
                f"""SELECT date, SUM(views) as views, SUM(subs) as subs
                   FROM {TAB_STAT}
                   WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                   GROUP BY date
                   ORDER BY date DESC""",
            )
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get daily statistics: {e}")
            return []