import asyncio
import hashlib
import logging
import time
from typing import Dict, Optional, List, Tuple
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    AuthKeyInvalidError, SessionPasswordNeededError,
    FloodWaitError, PhoneNumberInvalidError, RPCError
)

from config import API_ID, API_HASH, MAX_SESSIONS_IN_MEMORY, find_lang_code
from database import get_all_accounts, update_account_status
from exceptions import RateLimitError, SessionError, AuthorizationError, InvalidSessionError

logger = logging.getLogger(__name__)

class SessionManager:
    """
    Управление сессиями Telegram с предзагрузкой в RAM
    """
    
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}  # session_data -> client
        self.session_info: Dict[str, Dict] = {}       # session_data -> info
        self.loading_complete = False
        self.failed_sessions = set()
        self.max_sessions = MAX_SESSIONS_IN_MEMORY
        
    async def preload_all_sessions(self) -> Dict[str, int]:
        """
        Предзагружает ВСЕ сессии в оперативную память
        
        Returns:
            Dict с результатами загрузки
        """
        logger.info("🚀 Начинаю предзагрузку всех сессий в RAM...")
        start_time = time.time()
        
        results = {
            'total': 0,
            'loaded': 0,
            'failed': 0,
            'skipped': 0
        }
        
        try:
            # Получаем все аккаунты из БД
            all_accounts = await get_all_accounts(status='active')
            results['total'] = len(all_accounts)
            
            if not all_accounts:
                logger.warning("❌ Нет активных аккаунтов для загрузки")
                return results
            
            logger.info(f"📱 Найдено {results['total']} активных аккаунтов")
            
            # Загружаем батчами чтобы не перегрузить Telegram API
            batch_size = 100
            batch_delay = 15  # секунд между батчами
            
            for i in range(0, len(all_accounts), batch_size):
                batch = all_accounts[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(all_accounts) + batch_size - 1) // batch_size
                
                logger.info(f"⏳ Загружаю батч {batch_num}/{total_batches} ({len(batch)} аккаунтов)...")
                
                batch_results = await self._load_batch(batch)
                
                # Обновляем общие результаты
                results['loaded'] += batch_results['loaded']
                results['failed'] += batch_results['failed'] 
                results['skipped'] += batch_results['skipped']
                
                # Пауза между батчами (кроме последнего)
                if i + batch_size < len(all_accounts):
                    logger.info(f"😴 Пауза {batch_delay} секунд между батчами...")
                    await asyncio.sleep(batch_delay)
            
            self.loading_complete = True
            elapsed = time.time() - start_time
            
            logger.info(f"""
🎉 Предзагрузка завершена за {elapsed:.1f} секунд:
   ✅ Загружено: {results['loaded']}
   ❌ Ошибок: {results['failed']}
   ⏭️ Пропущено: {results['skipped']}
   📊 Успешность: {results['loaded']/results['total']*100:.1f}%
   💾 Занято RAM: ~{len(self.clients) * 0.2:.1f} МБ
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка предзагрузки: {e}")
            raise SessionError(f"Session preloading failed: {e}")
    
    async def _load_batch(self, accounts_batch: List[Dict]) -> Dict[str, int]:
        """Загружает батч аккаунтов параллельно"""
        results = {'loaded': 0, 'failed': 0, 'skipped': 0}
        
        # Создаем задачи для параллельной загрузки
        tasks = []
        for account in accounts_batch:
            if len(self.clients) >= self.max_sessions:
                results['skipped'] += 1
                continue
                
            task = asyncio.create_task(
                self._create_and_validate_client(account),
                name=f"load_{account['phone_number']}"
            )
            tasks.append((task, account))
        
        # Ждем завершения всех задач в батче
        if tasks:
            completed_tasks = await asyncio.gather(
                *[task for task, _ in tasks], 
                return_exceptions=True
            )
            
            # Анализируем результаты
            for (task, account), result in zip(tasks, completed_tasks):
                phone = account['phone_number']
                session_data = account['session_data']
                
                if isinstance(result, TelegramClient):
                    # Успешно загружен
                    self.clients[session_data] = result
                    self.session_info[session_data] = {
                        'phone': phone,
                        'lang': account['lang'],
                        'loaded_at': time.time(),
                        'status': 'active'
                    }
                    results['loaded'] += 1
                    logger.debug(f"✅ {phone}")
                    
                elif isinstance(result, Exception):
                    # Ошибка загрузки
                    self.failed_sessions.add(session_data)
                    results['failed'] += 1
                    logger.warning(f"❌ {phone}: {str(result)[:100]}")
                    
                    # Обновляем статус в БД если критическая ошибка
                    if isinstance(result, (AuthKeyInvalidError, InvalidSessionError)):
                        await update_account_status(phone, 'ban')
                        logger.info(f"🚫 {phone} помечен как забанен")
        
        return results
    
    async def _create_and_validate_client(self, account: Dict) -> TelegramClient:
        """Создает и валидирует клиент с retry логикой"""
        phone = account['phone_number']
        session_data = account['session_data']
        lang = account.get('lang', 'English')
        
        for attempt in range(3):  # 3 попытки
            client = None
            try:
                # Создаем клиент
                client = TelegramClient(
                    StringSession(session_data),
                    API_ID, API_HASH,
                    lang_code=find_lang_code(lang),
                    system_lang_code=find_lang_code(lang),
                    connection_retries=2,
                    request_retries=2,
                    timeout=20,
                    flood_sleep_threshold=30
                )
                
                # Подключаемся
                await client.connect()
                
                # Проверяем авторизацию
                if await client.is_user_authorized():
                    # Получаем информацию о пользователе для дополнительной проверки
                    try:
                        me = await client.get_me()
                        logger.debug(f"✅ Авторизован: {phone} (@{me.username or 'no_username'})")
                        return client
                    except Exception as e:
                        logger.warning(f"⚠️ {phone}: проблема получения профиля - {e}")
                        return client  # Все равно возвращаем, авторизация прошла
                else:
                    await client.disconnect()
                    raise AuthorizationError(f"Session not authorized for {phone}")
                    
            except FloodWaitError as e:
                if client:
                    await client.disconnect()
                    
                if attempt == 2:  # Последняя попытка
                    raise RateLimitError(f"FloodWait {e.seconds}s for {phone}", e.seconds)
                    
                # Ждем перед повтором
                wait_time = min(e.seconds, 60)  # Максимум 60 секунд
                logger.warning(f"⏳ {phone}: FloodWait {wait_time}s, попытка {attempt + 1}/3")
                await asyncio.sleep(wait_time)
                
            except (AuthKeyInvalidError, SessionPasswordNeededError) as e:
                if client:
                    await client.disconnect()
                raise InvalidSessionError(f"Invalid session for {phone}: {e}")
                
            except Exception as e:
                if client:
                    await client.disconnect()
                    
                if attempt == 2:  # Последняя попытка
                    raise SessionError(f"Failed to create client for {phone}: {e}")
                    
                # Ждем перед повтором
                await asyncio.sleep(2 ** attempt)
        
        raise SessionError(f"Max retries exceeded for {phone}")
    
    def get_client(self, session_data: str) -> Optional[TelegramClient]:
        """Получает готовый клиент из памяти"""
        if not self.loading_complete:
            logger.warning("⚠️ Сессии еще не загружены полностью")
            return None
            
        if session_data in self.failed_sessions:
            return None
            
        client = self.clients.get(session_data)
        if client and client.is_connected():
            return client
        elif client:
            # Клиент есть, но не подключен - удаляем из кэша
            self.clients.pop(session_data, None)
            self.session_info.pop(session_data, None)
            
        return None
    
    async def get_stats(self) -> Dict:
        """Получает статистику загруженных сессий"""
        connected_count = sum(
            1 for client in self.clients.values() 
            if client.is_connected()
        )
        
        return {
            'total_loaded': len(self.clients),
            'connected': connected_count,
            'disconnected': len(self.clients) - connected_count,
            'failed': len(self.failed_sessions),
            'loading_complete': self.loading_complete,
            'memory_usage_mb': len(self.clients) * 0.2  # Примерная оценка
        }
    
    async def health_check(self) -> Dict[str, int]:
        """Проверяет здоровье соединений"""
        if not self.loading_complete:
            return {'status': 'loading'}
            
        dead_sessions = []
        healthy_sessions = 0
        
        # Проверяем все клиенты
        for session_data, client in list(self.clients.items()):
            try:
                if client.is_connected():
                    # Дополнительная проверка - пингуем Telegram
                    await asyncio.wait_for(client.get_me(), timeout=5)
                    healthy_sessions += 1
                else:
                    dead_sessions.append(session_data)
            except Exception as e:
                logger.warning(f"Dead session detected: {self.session_info.get(session_data, {}).get('phone', 'unknown')}")
                dead_sessions.append(session_data)
        
        # Удаляем мертвые сессии
        for session_data in dead_sessions:
            self.clients.pop(session_data, None)
            self.session_info.pop(session_data, None)
            self.failed_sessions.add(session_data)
        
        return {
            'healthy': healthy_sessions,
            'removed_dead': len(dead_sessions),
            'total_active': len(self.clients)
        }
    
    async def add_new_session(self, account: Dict) -> bool:
        """Добавляет новую сессию в пул (для новых аккаунтов)"""
        if len(self.clients) >= self.max_sessions:
            logger.warning(f"⚠️ Достигнут лимит сессий {self.max_sessions}")
            return False
            
        try:
            client = await self._create_and_validate_client(account)
            session_data = account['session_data']
            
            self.clients[session_data] = client
            self.session_info[session_data] = {
                'phone': account['phone_number'],
                'lang': account['lang'],
                'loaded_at': time.time(),
                'status': 'active'
            }
            
            logger.info(f"➕ Добавлена новая сессия: {account['phone_number']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Не удалось добавить сессию {account['phone_number']}: {e}")
            return False
    
    async def remove_session(self, session_data: str):
        """Удаляет сессию из пула"""
        if session_data in self.clients:
            client = self.clients[session_data]
            try:
                if client.is_connected():
                    await client.disconnect()
            except:
                pass
            
            phone = self.session_info.get(session_data, {}).get('phone', 'unknown')
            self.clients.pop(session_data, None)
            self.session_info.pop(session_data, None)
            self.failed_sessions.discard(session_data)
            
            logger.info(f"➖ Удалена сессия: {phone}")
    
    async def shutdown(self):
        """Корректное закрытие всех соединений"""
        logger.info("🔄 Закрываю все сессии...")
        
        for session_data, client in list(self.clients.items()):
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception as e:
                logger.warning(f"Ошибка закрытия сессии: {e}")
        
        self.clients.clear()
        self.session_info.clear()
        self.failed_sessions.clear()
        self.loading_complete = False
        
        logger.info("✅ Все сессии закрыты")

# Глобальный экземпляр менеджера сессий
global_session_manager = SessionManager()