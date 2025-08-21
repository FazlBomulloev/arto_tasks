import asyncio
import logging
import zipfile
import shutil
import time
import os
import random
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import AuthKeyInvalidError, SessionPasswordNeededError

from opentele.td import TDesktop
from opentele.api import UseCurrentSession

from config import API_ID, API_HASH, DOWNLOADS_DIR, ACCOUNTS_DIR, find_english_word
from database import (
    add_account, get_account_by_phone, get_accounts_by_lang, 
    delete_accounts_by_status, get_account_stats, get_all_accounts
)
from exceptions import AccountValidationError, FileProcessingError

logger = logging.getLogger(__name__)

class AccountService:
    def __init__(self):
        self.validation_retries = 3
        self.validation_delay = 2.0
    
    async def add_accounts_from_zip(self, zip_path: Path, target_lang: str, 
                                  validate_accounts: bool = True,
                                  progress_callback=None) -> Dict[str, int]:
        results = {
            'total': 0,
            'validated': 0,
            'added': 0,
            'skipped_exists': 0,
            'failed_validation': 0,
            'failed_db': 0
        }
        
        temp_extract_path = None
        
        try:
            # 1. Извлекаем архив
            if progress_callback:
                await progress_callback("📦 Извлекаю архив...")
                
            temp_extract_path = DOWNLOADS_DIR / f"temp_extract_{int(time.time())}"
            accounts_data = await self._extract_zip_archive(zip_path, temp_extract_path)
            results['total'] = len(accounts_data)
            
            if not accounts_data:
                raise FileProcessingError("В архиве не найдено аккаунтов")
            
            logger.info(f"📱 Найдено {results['total']} аккаунтов в архиве")
            
            # 2. Проверяем существующие аккаунты
            if progress_callback:
                await progress_callback(f"🔍 Проверяю существующие аккаунты...")
                
            existing_phones = set()
            for account_data in accounts_data:
                existing = await get_account_by_phone(account_data['phone'])
                if existing:
                    results['skipped_exists'] += 1
                    existing_phones.add(account_data['phone'])
            
            # Фильтруем новые аккаунты
            new_accounts = [
                acc for acc in accounts_data 
                if acc['phone'] not in existing_phones
            ]
            
            logger.info(f"📝 К обработке: {len(new_accounts)} новых аккаунтов")
            
            if not new_accounts:
                return results
            
            # 3. Обрабатываем в выбранном режиме
            if validate_accounts:
                # Режим с проверкой
                batch_results = await self._process_with_validation(
                    new_accounts, target_lang, progress_callback
                )
            else:
                # Быстрый режим
                batch_results = await self._process_fast_mode(
                    new_accounts, target_lang, progress_callback
                )
            
            # Обновляем результаты
            for key in ['validated', 'added', 'failed_validation', 'failed_db']:
                results[key] += batch_results[key]
            
            # 4. Создаем задачи подписки для добавленных аккаунтов
            if batch_results.get('added_accounts'):
                if progress_callback:
                    await progress_callback(f"📺 Создаю задачи подписки...")
                
                subscription_stats = await self._create_subscription_tasks_for_new_accounts(
                    batch_results['added_accounts'], target_lang
                )
                
                logger.info(f"📺 Создано {subscription_stats.get('tasks_created', 0)} задач подписки")
            
            # 5. Финальный отчет
            success_rate = (results['added'] / results['total']) * 100 if results['total'] > 0 else 0
            mode_text = "с проверкой" if validate_accounts else "быстрое"
            
            logger.info(f"""
📊 Результат добавления аккаунтов ({mode_text}):
   📱 Всего в архиве: {results['total']}
   ✅ Валидировано: {results['validated']}
   ➕ Добавлено в БД: {results['added']}
   ⏭️ Уже существовало: {results['skipped_exists']}
   ❌ Не прошли валидацию: {results['failed_validation']}
   🚫 Ошибки БД: {results['failed_db']}
   📈 Успешность: {success_rate:.1f}%
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка добавления аккаунтов: {e}")
            raise AccountValidationError(f"Failed to add accounts: {e}")
            
        finally:
            # Очистка временных файлов
            if temp_extract_path and temp_extract_path.exists():
                try:
                    shutil.rmtree(temp_extract_path)
                except Exception as e:
                    logger.warning(f"Не удалось удалить временные файлы: {e}")
    
    async def _extract_zip_archive(self, zip_path: Path, extract_path: Path) -> List[Dict]:
        """Извлекает аккаунты из ZIP архива"""
        accounts_data = []
        
        try:
            extract_path.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            
            # Ищем папки с аккаунтами (содержащие tdata)
            for item in extract_path.iterdir():
                if item.is_dir():
                    tdata_path = item / 'tdata'
                    if tdata_path.exists() and tdata_path.is_dir():
                        # Проверяем что в tdata есть файлы
                        if any(tdata_path.glob('*')):
                            accounts_data.append({
                                'phone': item.name,
                                'tdata_path': tdata_path,
                                'account_path': item
                            })
            
            logger.info(f"📂 Извлечено {len(accounts_data)} папок с tdata")
            return accounts_data
            
        except zipfile.BadZipFile:
            raise FileProcessingError("Файл поврежден или не является ZIP архивом")
        except Exception as e:
            raise FileProcessingError(f"Ошибка извлечения архива: {e}")
    
    async def _process_with_validation(self, accounts_batch: List[Dict], target_lang: str, 
                                     progress_callback=None) -> Dict[str, int]:
        """Обрабатывает аккаунты с проверкой авторизации"""
        results = {
            'validated': 0,
            'added': 0,
            'failed_validation': 0,
            'failed_db': 0,
            'added_accounts': []
        }
        
        total_accounts = len(accounts_batch)
        logger.info(f"🔍 Режим с проверкой: {total_accounts} аккаунтов")
        
        for idx, account_data in enumerate(accounts_batch):
            phone = account_data['phone']
            
            try:
                if progress_callback and idx % 10 == 0:
                    progress = (idx / total_accounts) * 100
                    await progress_callback(f"🔍 Проверяю аккаунты: {idx}/{total_accounts} ({progress:.0f}%)")
                
                # 1. Конвертируем tdata в session_data
                session_data = await self._convert_tdata_to_session(account_data)
                
                if not session_data:
                    results['failed_validation'] += 1
                    logger.warning(f"❌ {phone}: не удалось конвертировать tdata")
                    continue
                
                # 2. ПРОВЕРЯЕМ авторизацию
                is_authorized = await self._validate_session_authorization(session_data, phone)
                
                if is_authorized:
                    results['validated'] += 1
                    
                    # 3. Добавляем в БД
                    success = await add_account(
                        phone=phone,
                        session_data=session_data,
                        lang=find_english_word(target_lang)
                    )
                    
                    if success:
                        results['added'] += 1
                        results['added_accounts'].append({
                            'phone_number': phone,
                            'session_data': session_data,
                            'lang': find_english_word(target_lang)
                        })
                        logger.debug(f"✅ {phone}: добавлен и проверен")
                    else:
                        results['failed_db'] += 1
                        logger.warning(f"🚫 {phone}: ошибка добавления в БД")
                else:
                    results['failed_validation'] += 1
                    logger.warning(f"❌ {phone}: не авторизован")
                
                # Небольшая задержка между проверками
                await asyncio.sleep(random.uniform(1.0, 3.0))
                
            except Exception as e:
                results['failed_validation'] += 1
                logger.error(f"💥 {phone}: ошибка обработки - {e}")
        
        return results
    
    async def _process_fast_mode(self, accounts_batch: List[Dict], target_lang: str, 
                               progress_callback=None) -> Dict[str, int]:
        """Обрабатывает аккаунты в быстром режиме без проверки"""
        results = {
            'validated': 0,
            'added': 0,
            'failed_validation': 0,
            'failed_db': 0,
            'added_accounts': []
        }
        
        total_accounts = len(accounts_batch)
        logger.info(f"⚡ Быстрый режим: {total_accounts} аккаунтов")
        
        for idx, account_data in enumerate(accounts_batch):
            phone = account_data['phone']
            
            try:
                if progress_callback and idx % 50 == 0:
                    progress = (idx / total_accounts) * 100
                    await progress_callback(f"⚡ Быстро добавляю: {idx}/{total_accounts} ({progress:.0f}%)")
                
                # 1. Конвертируем tdata в session_data БЕЗ подключения
                session_data = await self._convert_tdata_to_session_fast(account_data)
                
                if session_data:
                    # 2. Добавляем в БД БЕЗ проверки авторизации
                    success = await add_account(
                        phone=phone,
                        session_data=session_data,
                        lang=find_english_word(target_lang)
                    )
                    
                    if success:
                        results['added'] += 1
                        results['added_accounts'].append({
                            'phone_number': phone,
                            'session_data': session_data,
                            'lang': find_english_word(target_lang)
                        })
                        logger.debug(f"⚡ {phone}: быстро добавлен")
                    else:
                        results['failed_db'] += 1
                        logger.warning(f"🚫 {phone}: ошибка БД")
                else:
                    results['failed_db'] += 1
                    logger.warning(f"❌ {phone}: не удалось конвертировать")
                
            except Exception as e:
                results['failed_db'] += 1
                logger.error(f"💥 {phone}: ошибка быстрого добавления - {e}")
        
        return results
    
    async def _convert_tdata_to_session(self, account_data: Dict) -> Optional[str]:
        """Конвертирует tdata в session_data"""
        phone = account_data['phone']
        tdata_path = account_data['tdata_path']
        temp_session_path = None
        
        try:
            temp_session_path = tdata_path.parent / f"temp_{int(time.time())}.session"
            
            tdesk = TDesktop(tdata_path)
            if not tdesk.accounts:
                return None
            
            # Конвертируем в Telethon
            client = await tdesk.ToTelethon(
                session=str(temp_session_path),
                flag=UseCurrentSession
            )
            await client.disconnect()
            
            # Получаем session_data
            temp_client = TelegramClient(
                session=str(temp_session_path),
                api_id=API_ID,
                api_hash=API_HASH
            )
            
            session_data = StringSession.save(temp_client.session)
            logger.debug(f"🔄 {phone}: конвертирован в session_data")
            return session_data
            
        except Exception as e:
            logger.warning(f"❌ {phone}: ошибка конвертации - {e}")
            return None
            
        finally:
            if temp_session_path and temp_session_path.exists():
                try:
                    temp_session_path.unlink()
                except:
                    pass
    
    async def _convert_tdata_to_session_fast(self, account_data: Dict) -> Optional[str]:
        """Быстрая конвертация tdata в session_data БЕЗ подключения к Telegram"""
        return await self._convert_tdata_to_session(account_data)
    
    async def _validate_session_authorization(self, session_data: str, phone: str) -> bool:
        """Проверяет авторизацию сессии путем подключения к Telegram"""
        client = None
        
        for attempt in range(self.validation_retries):
            try:
                # Создаем клиент
                client = TelegramClient(
                    StringSession(session_data),
                    API_ID, API_HASH,
                    connection_retries=1,
                    timeout=15
                )
                
                # Подключаемся
                await client.connect()
                
                # Проверяем авторизацию
                if await client.is_user_authorized():
                    # Дополнительная проверка - получаем профиль
                    try:
                        me = await client.get_me()
                        logger.debug(f"✅ {phone}: авторизован (@{me.username or 'no_username'})")
                        return True
                    except Exception as e:
                        logger.warning(f"⚠️ {phone}: проблема получения профиля - {e}")
                        return True  # Все равно считаем авторизованным
                else:
                    logger.warning(f"❌ {phone}: не авторизован")
                    return False
                    
            except Exception as e:
                logger.warning(f"❌ {phone}: ошибка проверки (попытка {attempt + 1}/{self.validation_retries}) - {e}")
                
                if attempt < self.validation_retries - 1:
                    await asyncio.sleep(self.validation_delay)
                    
            finally:
                if client:
                    try:
                        await client.disconnect()
                    except:
                        pass
        
        return False
    
    async def _create_subscription_tasks_for_new_accounts(self, added_accounts: List[Dict], target_lang: str) -> Dict[str, int]:
        """Создает задачи подписки для новых аккаунтов на все каналы языка"""
        subscription_stats = {
            'channels_found': 0,
            'tasks_created': 0,
            'accounts_processed': 0
        }
        
        try:
            from database import get_channels_by_lang
            channels = await get_channels_by_lang(target_lang)
            
            if not channels:
                logger.info(f"📺 Нет каналов для языка {target_lang}")
                return subscription_stats
            
            subscription_stats['channels_found'] = len(channels)
            subscription_stats['accounts_processed'] = len(added_accounts)
            
            logger.info(f"📺 Создание задач подписки: {len(added_accounts)} аккаунтов на {len(channels)} каналов")
            
            # Создаем задачи для каждого канала
            total_tasks_created = 0
            
            for channel_name in channels:
                try:
                    channel_tasks = await self._create_subscription_tasks_for_channel(
                        channel_name, added_accounts, target_lang
                    )
                    
                    total_tasks_created += channel_tasks
                    logger.debug(f"✅ Канал @{channel_name}: {channel_tasks} задач")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка создания задач для @{channel_name}: {e}")
            
            subscription_stats['tasks_created'] = total_tasks_created
            
            logger.info(f"📊 Создано {total_tasks_created} задач подписки для новых аккаунтов")
            return subscription_stats
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач подписки: {e}")
            return subscription_stats
    
    async def _create_subscription_tasks_for_channel(self, channel_name: str, accounts: List[Dict], target_lang: str) -> int:
        """Создает задачи подписки на один канал для списка аккаунтов"""
        try:
            if not accounts:
                return 0
            
            from config import find_english_word, read_setting
            import time
            import random
            
            english_lang = find_english_word(target_lang)
            
            # Получаем параметры задержек
            base_delay = read_setting('lag.txt', 14.0) * 60
            range_val = read_setting('range.txt', 5.0) * 60
            timeout_count = int(read_setting('timeout_count.txt', 3.0))
            timeout_duration = read_setting('timeout_duration.txt', 13.0) * 60
            
            # Перемешиваем аккаунты
            shuffled_accounts = accounts.copy()
            random.shuffle(shuffled_accounts)
            
            # Создаем задачи с временными метками
            subscription_tasks = []
            current_time = time.time()
            next_execute_time = current_time
            
            for account_idx, account in enumerate(shuffled_accounts):
                # Рассчитываем время выполнения
                if account_idx == 0:
                    execute_at = next_execute_time
                else:
                    # Проверяем нужна ли пауза
                    if account_idx % timeout_count == 0:
                        pause_delay = timeout_duration
                    else:
                        pause_delay = 0
                    
                    # Базовая задержка + разброс + пауза
                    random_variation = random.uniform(-range_val, range_val)
                    total_delay = base_delay + random_variation + pause_delay
                    
                    execute_at = next_execute_time + total_delay
                
                next_execute_time = execute_at
                
                task_data = {
                    'account_session': account['session_data'],
                    'phone': account['phone_number'],
                    'channel': channel_name,
                    'lang': english_lang,
                    'task_type': 'subscribe',
                    'execute_at': execute_at,
                    'retry_count': 0,
                    'created_at': time.time()
                }
                
                subscription_tasks.append(task_data)
            
            # Сохраняем в Redis
            await self._save_tasks_to_redis(subscription_tasks)
            
            return len(subscription_tasks)
            
        except Exception as e:
            logger.error(f"Ошибка создания задач подписки для канала {channel_name}: {e}")
            return 0
    
    async def _save_tasks_to_redis(self, tasks: List[Dict]):
        """Сохраняет задачи в Redis"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            import json
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            # Подготавливаем данные для Redis
            tasks_data = {}
            
            for task in tasks:
                task_json = json.dumps(task)
                execute_at = task['execute_at']
                tasks_data[task_json] = execute_at
            
            # Сохраняем в единую очередь
            if tasks_data:
                redis_client.zadd("task_queue", tasks_data)
                redis_client.expire("task_queue", 48 * 3600)  # TTL 48 часов
                
                logger.debug(f"📋 Сохранено {len(tasks)} задач в Redis")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения задач в Redis: {e}")
    
    # ========== УПРАВЛЕНИЕ АККАУНТАМИ ==========
    
    async def delete_accounts_by_status(self, status: str, limit: int = None) -> int:
        """Удаляет аккаунты по статусу"""
        try:
            from database import delete_accounts_by_status as db_delete_accounts_by_status
            deleted_count = await db_delete_accounts_by_status(status, limit)
            
            logger.info(f"🗑️ Удалено {deleted_count} аккаунтов со статусом '{status}'")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Ошибка удаления аккаунтов: {e}")
            return 0
    
    async def export_active_accounts(self, target_lang: Optional[str] = None) -> Optional[Path]:
        """Экспортирует активные аккаунты в ZIP архив"""
        try:
            # Получаем активные аккаунты
            if target_lang:
                accounts = await get_accounts_by_lang(find_english_word(target_lang), 'active')
                archive_name = f"active_accounts_{target_lang}_{int(time.time())}.zip"
            else:
                accounts = await get_all_accounts('active')
                archive_name = f"active_accounts_all_{int(time.time())}.zip"
            
            if not accounts:
                logger.warning("❌ Нет активных аккаунтов для экспорта")
                return None
            
            # Создаем архив с session_data
            archive_path = DOWNLOADS_DIR / archive_name
            temp_dir = DOWNLOADS_DIR / f"temp_export_{int(time.time())}"
            
            try:
                temp_dir.mkdir(exist_ok=True)
                
                # Создаем структуру аккаунтов
                exported_count = 0
                for account in accounts:
                    phone = account['phone_number']
                    session_data = account['session_data']
                    lang = account.get('lang', 'none')
                    
                    # Создаем папку для аккаунта
                    account_dir = temp_dir / phone
                    account_dir.mkdir(exist_ok=True)
                    
                    # Создаем session файл
                    session_file = account_dir / f"{phone}.session"
                    
                    try:
                        # Создаем клиент для сохранения session
                        temp_client = TelegramClient(
                            str(session_file),
                            API_ID, API_HASH,
                            session=StringSession(session_data)
                        )
                        
                        # Сохраняем session в файл
                        await temp_client.connect()
                        await temp_client.disconnect()
                        
                        # Создаем info файл
                        info_file = account_dir / "info.txt"
                        info_content = f"""Phone: {phone}
Language: {lang}
Status: {account.get('status', 'active')}
Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}
Session: {session_data[:50]}...
"""
                        info_file.write_text(info_content, encoding='utf-8')
                        
                        exported_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось экспортировать {phone}: {e}")
                        continue
                
                if exported_count == 0:
                    logger.error("❌ Не удалось экспортировать ни одного аккаунта")
                    return None
                
                # Создаем ZIP архив
                import zipfile
                with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            file_path = Path(root) / file
                            arc_path = file_path.relative_to(temp_dir)
                            zipf.write(file_path, arc_path)
                
                # Очищаем временную папку
                shutil.rmtree(temp_dir)
                
                logger.info(f"📦 Экспортировано {exported_count} активных аккаунтов в {archive_name}")
                return archive_path
                
            except Exception as e:
                # Очищаем временные файлы при ошибке
                if temp_dir.exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)
                if archive_path.exists():
                    archive_path.unlink()
                raise e
                
        except Exception as e:
            logger.error(f"💥 Ошибка экспорта аккаунтов: {e}")
            return None

# Глобальный экземпляр сервиса
account_service = AccountService()