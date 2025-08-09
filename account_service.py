import asyncio
import logging
import zipfile
import shutil
import time
import os
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
from session_manager import global_session_manager
from exceptions import AccountValidationError, FileProcessingError

logger = logging.getLogger(__name__)

class AccountService:
    """Сервис для управления аккаунтами"""
    
    def __init__(self):
        self.validation_retries = 5
        self.validation_delay = 3.0
    
    async def add_accounts_from_zip(self, zip_path: Path, target_lang: str, 
                                  progress_callback=None) -> Dict[str, int]:
        """
        Добавляет аккаунты из ZIP с ОБЯЗАТЕЛЬНОЙ авторизацией
        
        Returns:
            Dict с результатами валидации и добавления
        """
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
            
            # 3. Валидируем и добавляем аккаунты батчами
            batch_size = 20  # Небольшие батчи для стабильности
            
            for i in range(0, len(new_accounts), batch_size):
                batch = new_accounts[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(new_accounts) + batch_size - 1) // batch_size
                
                if progress_callback:
                    await progress_callback(
                        f"🔐 Валидирую батч {batch_num}/{total_batches} ({len(batch)} аккаунтов)..."
                    )
                
                batch_results = await self._validate_and_add_batch(batch, target_lang)
                
                # Обновляем результаты
                results['validated'] += batch_results['validated']
                results['added'] += batch_results['added']
                results['failed_validation'] += batch_results['failed_validation']
                results['failed_db'] += batch_results['failed_db']
                
                # Пауза между батчами
                if i + batch_size < len(new_accounts):
                    await asyncio.sleep(10)  # 10 секунд между батчами
            
            # 4. Финальный отчет
            success_rate = (results['added'] / results['total']) * 100 if results['total'] > 0 else 0
            
            logger.info(f"""
📊 Результат добавления аккаунтов:
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
    
    async def _validate_and_add_batch(self, accounts_batch: List[Dict], 
                                    target_lang: str) -> Dict[str, int]:
        """Валидирует и добавляет батч аккаунтов С ЗАДЕРЖКАМИ"""
        results = {
            'validated': 0,
            'added': 0,
            'failed_validation': 0,
            'failed_db': 0
        }
        
        # Рассчитываем задержки для постепенного подключения
        total_accounts = len(accounts_batch)
        total_time = 2 * 3600  # 2 часа в секундах для 1к аккаунтов
        base_delay = total_time / max(total_accounts, 1)  # Базовая задержка между аккаунтами
        
        logger.info(f"🐌 Постепенное подключение {total_accounts} аккаунтов за {total_time/3600:.1f} часов")
        logger.info(f"⏱️ Базовая задержка между аккаунтами: {base_delay:.1f} секунд")
        
        # Обрабатываем аккаунты ПОСЛЕДОВАТЕЛЬНО с задержками
        for idx, account_data in enumerate(accounts_batch):
            phone = account_data['phone']
            
            try:
                # Валидируем аккаунт
                session_data = await self._validate_single_account(account_data)
                results['validated'] += 1
                
                # Добавляем в БД
                success = await add_account(
                    phone=phone,
                    session_data=session_data,
                    lang=find_english_word(target_lang)
                )
                
                if success:
                    results['added'] += 1
                    logger.info(f"✅ {phone} добавлен ({idx + 1}/{total_accounts})")
                    
                    # Добавляем в пул сессий если он загружен
                    if global_session_manager.loading_complete:
                        account_dict = {
                            'phone_number': phone,
                            'session_data': session_data,
                            'lang': find_english_word(target_lang)
                        }
                        await global_session_manager.add_new_session(account_dict)
                else:
                    results['failed_db'] += 1
                    logger.error(f"🚫 {phone} не удалось добавить в БД")
                
                # ВАЖНО: Задержка между аккаунтами
                if idx < total_accounts - 1:  # Не ждем после последнего
                    # Добавляем рандомизацию ±20%
                    delay = base_delay * random.uniform(0.8, 1.2)
                    logger.debug(f"⏳ Пауза {delay:.1f}с перед следующим аккаунтом...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                # Ошибка валидации
                results['failed_validation'] += 1
                error_msg = str(e) if e else "Unknown error"
                logger.warning(f"❌ {phone} не прошел валидацию: {error_msg[:100]}")
                
                # Небольшая задержка даже при ошибке
                if idx < total_accounts - 1:
                    await asyncio.sleep(5)
        
        logger.info(f"🏁 Завершена обработка батча: {results['added']} из {total_accounts} добавлено")
        return results
    
    async def _validate_single_account(self, account_data: Dict) -> str:
        """
        Валидирует один аккаунт с retry логикой
        
        Returns:
            session_data (str) если успешно, иначе raises Exception
        """
        phone = account_data['phone']
        tdata_path = account_data['tdata_path']
        
        for attempt in range(self.validation_retries):
            temp_session_path = None
            client = None
            
            try:
                # 1. Конвертируем tdata в сессию
                temp_session_path = tdata_path.parent / f"temp_{int(time.time())}.session"
                
                tdesk = TDesktop(tdata_path)
                if not tdesk.accounts:
                    raise AccountValidationError(f"No valid accounts in tdata for {phone}")
                
                # Конвертируем в Telethon
                client = await tdesk.ToTelethon(
                    session=str(temp_session_path),
                    flag=UseCurrentSession
                )
                await client.disconnect()
                
                # 2. Создаем новый клиент для проверки
                client = TelegramClient(
                    session=str(temp_session_path),
                    api_id=API_ID,
                    api_hash=API_HASH,
                    connection_retries=2,
                    timeout=15
                )
                
                await client.connect()
                
                # 3. Проверяем авторизацию
                if not await client.is_user_authorized():
                    raise AccountValidationError(f"Session not authorized for {phone}")
                
                # 4. Дополнительная проверка - получаем профиль
                try:
                    me = await client.get_me()
                    if me.phone and me.phone != phone:
                        logger.warning(f"⚠️ Phone mismatch for {phone}: expected {phone}, got {me.phone}")
                except Exception as e:
                    logger.warning(f"⚠️ {phone}: couldn't get profile - {e}")
                
                # 5. Получаем session_data
                session_data = StringSession.save(client.session)
                
                await client.disconnect()
                
                logger.debug(f"✅ {phone} прошел валидацию (попытка {attempt + 1})")
                return session_data
                
            except Exception as e:
                if client:
                    try:
                        await client.disconnect()
                    except:
                        pass
                
                # Удаляем временный файл сессии
                if temp_session_path and temp_session_path.exists():
                    try:
                        temp_session_path.unlink()
                    except:
                        pass
                
                if attempt == self.validation_retries - 1:
                    # Последняя попытка
                    raise AccountValidationError(f"Validation failed for {phone}: {e}")
                
                # Ждем перед повтором
                wait_time = self.validation_delay * (2 ** attempt)  # Экспоненциальная задержка
                logger.debug(f"⏳ {phone} retry {attempt + 1}/{self.validation_retries} after {wait_time}s")
                await asyncio.sleep(wait_time)
                
            finally:
                # Очистка временных файлов
                if temp_session_path and temp_session_path.exists():
                    try:
                        temp_session_path.unlink()
                    except:
                        pass
        
        raise AccountValidationError(f"Max validation retries exceeded for {phone}")
    
    async def add_accounts_from_zip_with_validation(self, zip_path: Path, target_lang: str, 
                                                  progress_callback=None) -> Dict[str, int]:
        """
        Добавляет аккаунты из ZIP с ОБЯЗАТЕЛЬНОЙ валидацией (медленно)
        Это старый метод с проверкой
        """
        return await self.add_accounts_from_zip(zip_path, target_lang, progress_callback)
    
    async def add_accounts_from_zip_fast(self, zip_path: Path, target_lang: str, 
                                       progress_callback=None) -> Dict[str, int]:
        """
        Быстрое добавление аккаунтов БЕЗ проверки авторизации
        
        Returns:
            Dict с результатами добавления
        """
        results = {
            'total': 0,
            'added': 0,
            'skipped_exists': 0,
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
            
            logger.info(f"📱 Найдено {results['total']} аккаунтов в архиве (быстрый режим)")
            
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
            
            logger.info(f"📝 К добавлению: {len(new_accounts)} новых аккаунтов")
            
            if not new_accounts:
                return results
            
            # 3. Быстрое добавление БЕЗ валидации
            if progress_callback:
                await progress_callback(f"⚡ Быстро добавляю {len(new_accounts)} аккаунтов...")
            
            for idx, account_data in enumerate(new_accounts):
                try:
                    phone = account_data['phone']
                    
                    # Конвертируем tdata в session_data БЕЗ подключения к Telegram
                    session_data = await self._convert_tdata_to_session_fast(account_data)
                    
                    if session_data:
                        # Добавляем в БД со статусом 'none' (не проверен)
                        success = await add_account(
                            phone=phone,
                            session_data=session_data,
                            lang=find_english_word(target_lang)
                        )
                        
                        if success:
                            results['added'] += 1
                            if (idx + 1) % 100 == 0:
                                logger.info(f"➕ Добавлено {idx + 1}/{len(new_accounts)} аккаунтов")
                        else:
                            results['failed_db'] += 1
                    else:
                        results['failed_db'] += 1
                        
                except Exception as e:
                    logger.error(f"Ошибка добавления {phone}: {e}")
                    results['failed_db'] += 1
            
            # 4. Финальный отчет
            logger.info(f"""
📊 Результат быстрого добавления аккаунтов:
   📱 Всего в архиве: {results['total']}
   ➕ Добавлено в БД: {results['added']}
   ⏭️ Уже существовало: {results['skipped_exists']}
   🚫 Ошибки БД: {results['failed_db']}
   ⚡ Режим: без проверки авторизации
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка быстрого добавления аккаунтов: {e}")
            raise AccountValidationError(f"Failed to add accounts fast: {e}")
            
        finally:
            # Очистка временных файлов
            if temp_extract_path and temp_extract_path.exists():
                try:
                    shutil.rmtree(temp_extract_path)
                except Exception as e:
                    logger.warning(f"Не удалось удалить временные файлы: {e}")
    
    async def _convert_tdata_to_session_fast(self, account_data: Dict) -> Optional[str]:
        """
        Быстрая конвертация tdata в session_data БЕЗ подключения к Telegram
        """
        phone = account_data['phone']
        tdata_path = account_data['tdata_path']
        temp_session_path = None
        
        try:
            # Конвертируем tdata в сессию БЕЗ подключения
            temp_session_path = tdata_path.parent / f"temp_{int(time.time())}.session"
            
            tdesk = TDesktop(tdata_path)
            if not tdesk.accounts:
                return None
            
            # Конвертируем в Telethon БЕЗ подключения
            client = await tdesk.ToTelethon(
                session=str(temp_session_path),
                flag=UseCurrentSession
            )
            await client.disconnect()
            
            # Получаем session_data из файла
            from telethon.sessions import SQLiteSession
            temp_session = SQLiteSession(str(temp_session_path))
            
            # Создаем StringSession из SQLite сессии
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            
            temp_client = TelegramClient(
                temp_session, API_ID, API_HASH
            )
            
            # Получаем session_data
            session_data = StringSession.save(temp_client.session)
            
            logger.debug(f"⚡ {phone}: быстро конвертирован в session_data")
            return session_data
            
        except Exception as e:
            logger.warning(f"❌ {phone}: ошибка быстрой конвертации - {e}")
            return None
            
        finally:
            # Очистка временных файлов
            if temp_session_path and temp_session_path.exists():
                try:
                    temp_session_path.unlink()
                except:
                    pass
        """
        Удаляет аккаунты по статусу
        
        Args:
            status: статус для удаления ('ban', 'pause', etc.)
            limit: максимальное количество для удаления
            
        Returns:
            количество удаленных аккаунтов
        """
        try:
            # Получаем аккаунты которые будем удалять
            accounts_to_delete = await get_accounts_by_lang('*', status) if status != 'all' else await get_all_accounts()
            
            if limit:
                accounts_to_delete = accounts_to_delete[:limit]
            
            if not accounts_to_delete:
                logger.info(f"❌ Нет аккаунтов со статусом '{status}' для удаления")
                return 0
            
            # Удаляем из пула сессий
            for account in accounts_to_delete:
                await global_session_manager.remove_session(account['session_data'])
            
            # Удаляем из БД
            deleted_count = await delete_accounts_by_status(status, limit)
            
            logger.info(f"🗑️ Удалено {deleted_count} аккаунтов со статусом '{status}'")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Ошибка удаления аккаунтов: {e}")
            return 0
    
    async def export_active_accounts(self, target_lang: Optional[str] = None) -> Optional[Path]:
        """
        Экспортирует активные аккаунты в ZIP архив с session_data (не файлами)
        
        Args:
            target_lang: экспортировать только указанный язык (опционально)
            
        Returns:
            Path к созданному архиву или None если ошибка
        """
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
                
                # Создаем структуру аккаунтов из session_data
                exported_count = 0
                for account in accounts:
                    phone = account['phone_number']
                    session_data = account['session_data']
                    lang = account.get('lang', 'none')
                    
                    # Создаем папку для аккаунта
                    account_dir = temp_dir / phone
                    account_dir.mkdir(exist_ok=True)
                    
                    # Создаем файл с session data
                    session_file = account_dir / f"{phone}.session"
                    
                    # Создаем telethon session из StringSession
                    try:
                        from telethon.sessions import StringSession
                        from telethon import TelegramClient
                        
                        # Временно создаем клиент для сохранения session файла
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
