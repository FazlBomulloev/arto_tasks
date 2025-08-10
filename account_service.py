import asyncio
import logging
import zipfile
import shutil
import time
import os
import random
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

# Импорты для создания задач подписки
try:
    from task_service import TaskItem, TaskType
except ImportError:
    # Если task_service еще не загружен, создаем локальные определения
    from dataclasses import dataclass
    from enum import Enum
    
    class TaskType(Enum):
        SUBSCRIBE = "subscribe"
    
    @dataclass
    class TaskItem:
        account_session: str
        phone: str
        channel: str  
        lang: str
        task_type: TaskType
        execute_at: Optional[float] = None
        retry_count: int = 0

logger = logging.getLogger(__name__)

class AccountService:
    """Сервис для управления аккаунтами"""
    
    def __init__(self):
        self.validation_retries = 5
        self.validation_delay = 3.0
    
    async def add_accounts_from_zip(self, zip_path: Path, target_lang: str, 
                                  progress_callback=None) -> Dict[str, int]:
        """
        Добавляет аккаунты из ZIP с АДАПТИВНОЙ валидацией
        
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
            
            # 3. АДАПТИВНАЯ валидация и добавление
            batch_results = await self._validate_and_add_batch_adaptive(new_accounts, target_lang, progress_callback)
            
            # Обновляем результаты
            for key in ['validated', 'added', 'failed_validation', 'failed_db']:
                results[key] += batch_results[key]
            
            # 4. Финальный отчет
            success_rate = (results['added'] / results['total']) * 100 if results['total'] > 0 else 0
            
            logger.info(f"""
📊 Результат АДАПТИВНОГО добавления аккаунтов:
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
    
    async def _validate_and_add_batch_adaptive(self, accounts_batch: List[Dict], 
                                             target_lang: str, progress_callback=None) -> Dict[str, int]:
        """
        АДАПТИВНАЯ валидация с автоматическим расчетом времени
        """
        results = {
            'validated': 0,
            'added': 0,
            'failed_validation': 0,
            'failed_db': 0,
            'added_accounts': []  # Для отслеживания добавленных аккаунтов
        }
        
        total_accounts = len(accounts_batch)
        
        # ========== АДАПТИВНЫЕ ПАРАМЕТРЫ ПОД КОЛИЧЕСТВО ==========
        
        processing_params = self._calculate_adaptive_params(total_accounts)
        
        logger.info(f"""
🎯 АДАПТИВНАЯ обработка {total_accounts} аккаунтов:
   ⏱️ Расчетное время: {processing_params['estimated_hours']:.1f} часов
   ⚡ Параллельных потоков: {processing_params['parallel_batch_size']}
   🔄 Задержка между аккаунтами: {processing_params['account_delay']:.1f} сек
   ⏸️ Пауза между батчами: {processing_params['batch_pause']} сек
   🎚️ Режим: {processing_params['mode']}
        """)
        
        # Обновляем прогресс с информацией о времени
        if progress_callback:
            await progress_callback(
                f"🎯 Режим: {processing_params['mode']} | "
                f"Время: ~{processing_params['estimated_hours']:.1f}ч | "
                f"Потоков: {processing_params['parallel_batch_size']}"
            )
        
        # Выбираем стратегию обработки в зависимости от количества
        if processing_params['mode'] == 'instant':
            # Мгновенная обработка для маленького количества
            batch_results = await self._process_instant_mode(accounts_batch, target_lang, processing_params, progress_callback)
        
        elif processing_params['mode'] == 'fast_parallel':
            # Быстрая параллельная для среднего количества
            batch_results = await self._process_fast_parallel_mode(accounts_batch, target_lang, processing_params, progress_callback)
        
        else:  # 'distributed'
            # Распределенная для большого количества
            batch_results = await self._process_distributed_mode(accounts_batch, target_lang, processing_params, progress_callback)
        
        # Обновляем результаты
        for key in ['validated', 'added', 'failed_validation', 'failed_db']:
            results[key] = batch_results.get(key, 0)
        
        return results

    def _calculate_adaptive_params(self, total_accounts: int) -> Dict:
        """
        Рассчитывает оптимальные параметры в зависимости от количества аккаунтов
        """
        
        if total_accounts <= 50:
            # ========== МАЛОЕ КОЛИЧЕСТВО (1-50): МГНОВЕННО ==========
            return {
                'mode': 'instant',
                'estimated_hours': 0.1,  # 6 минут максимум
                'parallel_batch_size': min(total_accounts, 10),
                'account_delay': 1.0,    # 1 секунда между аккаунтами
                'batch_pause': 5,        # 5 секунд между батчами
                'retry_count': 2,        # Меньше попыток для скорости
                'timeout': 8             # Быстрые таймауты
            }
        
        elif total_accounts <= 500:
            # ========== СРЕДНЕЕ КОЛИЧЕСТВО (51-500): БЫСТРО ==========
            estimated_time = 0.02 * total_accounts  # ~2 минуты на 100 аккаунтов
            
            return {
                'mode': 'fast_parallel', 
                'estimated_hours': estimated_time / 60,
                'parallel_batch_size': min(total_accounts // 10, 25),  # 10-25 потоков
                'account_delay': 2.0,    # 2 секунды между аккаунтами
                'batch_pause': 15,       # 15 секунд между батчами
                'retry_count': 3,        # Средняя надежность
                'timeout': 12            # Средние таймауты
            }
        
        elif total_accounts <= 2000:
            # ========== БОЛЬШОЕ КОЛИЧЕСТВО (501-2000): УМЕРЕННО ==========
            estimated_time = 0.05 * total_accounts  # ~5 минут на 100 аккаунтов
            
            return {
                'mode': 'distributed',
                'estimated_hours': estimated_time / 60,
                'parallel_batch_size': min(total_accounts // 20, 40),  # 20-40 потоков
                'account_delay': 3.0,    # 3 секунды между аккаунтами  
                'batch_pause': 30,       # 30 секунд между батчами
                'retry_count': 3,        # Хорошая надежность
                'timeout': 15            # Надежные таймауты
            }
        
        else:
            # ========== ОЧЕНЬ БОЛЬШОЕ (2000+): КОНСЕРВАТИВНО ==========
            # Линейная зависимость: ~1 час на 1000 аккаунтов
            estimated_time = total_accounts * 1.0  # 1 час на 1000 аккаунтов
            
            return {
                'mode': 'distributed',
                'estimated_hours': estimated_time / 1000,
                'parallel_batch_size': min(total_accounts // 50, 50),  # 20-50 потоков
                'account_delay': 5.0,    # 5 секунд между аккаунтами
                'batch_pause': 60,       # 1 минута между батчами
                'retry_count': 5,        # Максимальная надежность
                'timeout': 20            # Длинные таймауты
            }

    async def _process_instant_mode(self, accounts_batch: List[Dict], target_lang: str, 
                                  params: Dict, progress_callback=None) -> Dict[str, int]:
        """
        МГНОВЕННЫЙ режим для малого количества аккаунтов (1-50)
        Все аккаунты обрабатываются почти одновременно
        """
        results = {
            'validated': 0, 
            'added': 0, 
            'failed_validation': 0, 
            'failed_db': 0,
            'added_accounts': []
        }
        
        logger.info(f"⚡ МГНОВЕННЫЙ режим: {len(accounts_batch)} аккаунтов")
        
        if progress_callback:
            await progress_callback("⚡ Мгновенная обработка всех аккаунтов...")
        
        # Создаем задачи для всех аккаунтов сразу с минимальными задержками
        validation_tasks = []
        
        for idx, account_data in enumerate(accounts_batch):
            # Очень маленькие задержки чтобы не нагружать API
            start_delay = idx * params['account_delay']
            
            task = asyncio.create_task(
                self._validate_and_add_single_optimized(
                    account_data, target_lang, start_delay, params
                )
            )
            validation_tasks.append(task)
        
        # Ждем завершения всех задач
        batch_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        # Анализируем результаты
        for idx, result in enumerate(batch_results):
            phone = accounts_batch[idx]['phone']
            
            if isinstance(result, dict) and result.get('success'):
                results['validated'] += 1
                results['added'] += 1
                if 'account_data' in result:
                    results['added_accounts'].append(result['account_data'])
            elif isinstance(result, Exception):
                results['failed_validation'] += 1
                logger.warning(f"❌ {phone}: {str(result)[:50]}")
            else:
                results['failed_db'] += 1
        
        # СОЗДАЕМ ЗАДАЧИ ПОДПИСКИ для добавленных аккаунтов
        if results['added_accounts']:
            if progress_callback:
                await progress_callback("📺 Создаю задачи подписки для новых аккаунтов...")
            
            subscription_stats = await self._create_subscription_tasks_for_new_accounts(
                results['added_accounts'], target_lang
            )
            
            logger.info(f"📺 Создано {subscription_stats['tasks_created']} задач подписки")
        
        return results

    async def _process_fast_parallel_mode(self, accounts_batch: List[Dict], target_lang: str, 
                                        params: Dict, progress_callback=None) -> Dict[str, int]:
        """
        БЫСТРЫЙ ПАРАЛЛЕЛЬНЫЙ режим для среднего количества (51-500)
        Обработка небольшими батчами с контролируемой параллельностью
        """
        results = {
            'validated': 0, 
            'added': 0, 
            'failed_validation': 0, 
            'failed_db': 0,
            'added_accounts': []
        }
        
        batch_size = params['parallel_batch_size']
        total_accounts = len(accounts_batch)
        
        logger.info(f"🚀 БЫСТРЫЙ ПАРАЛЛЕЛЬНЫЙ режим: {total_accounts} аккаунтов, батчи по {batch_size}")
        
        # Обрабатываем параллельными батчами
        for i in range(0, total_accounts, batch_size):
            batch = accounts_batch[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_accounts + batch_size - 1) // batch_size
            
            logger.info(f"📦 Батч {batch_num}/{total_batches}: {len(batch)} аккаунтов")
            
            if progress_callback:
                progress = (i / total_accounts) * 100
                await progress_callback(f"🚀 Батч {batch_num}/{total_batches} ({progress:.0f}%)")
            
            # Валидируем батч параллельно
            batch_results = await self._validate_parallel_batch_optimized(batch, target_lang, params)
            
            # Обновляем результаты
            for key in ['validated', 'added', 'failed_validation', 'failed_db']:
                results[key] += batch_results[key]
            
            # Собираем добавленные аккаунты
            results['added_accounts'].extend(batch_results.get('added_accounts', []))
            
            # Пауза между батчами (кроме последнего)
            if i + batch_size < total_accounts:
                logger.info(f"⏳ Пауза {params['batch_pause']} сек между батчами...")
                await asyncio.sleep(params['batch_pause'])
        
        # СОЗДАЕМ ЗАДАЧИ ПОДПИСКИ для всех добавленных аккаунтов
        if results['added_accounts']:
            if progress_callback:
                await progress_callback("📺 Создаю задачи подписки для новых аккаунтов...")
            
            subscription_stats = await self._create_subscription_tasks_for_new_accounts(
                results['added_accounts'], target_lang
            )
            
            logger.info(f"📺 Создано {subscription_stats['tasks_created']} задач подписки")
        
        return results

    async def _process_distributed_mode(self, accounts_batch: List[Dict], target_lang: str, 
                                      params: Dict, progress_callback=None) -> Dict[str, int]:
        """
        РАСПРЕДЕЛЕННЫЙ режим для большого количества (500+)
        Постепенная обработка с учетом лимитов Telegram API
        """
        results = {
            'validated': 0, 
            'added': 0, 
            'failed_validation': 0, 
            'failed_db': 0,
            'added_accounts': []
        }
        
        batch_size = params['parallel_batch_size']
        total_accounts = len(accounts_batch)
        
        logger.info(f"🏗️ РАСПРЕДЕЛЕННЫЙ режим: {total_accounts} аккаунтов, батчи по {batch_size}")
        logger.info(f"⏱️ Расчетное время: {params['estimated_hours']:.1f} часов")
        
        # Добавляем прогрессивное увеличение задержек для больших объемов
        current_delay = params['account_delay']
        
        # Обрабатываем консервативными батчами
        for i in range(0, total_accounts, batch_size):
            batch = accounts_batch[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_accounts + batch_size - 1) // batch_size
            
            # Прогресс
            progress = (i / total_accounts) * 100
            logger.info(f"📦 Батч {batch_num}/{total_batches}: {len(batch)} аккаунтов ({progress:.1f}%)")
            
            if progress_callback:
                await progress_callback(f"🏗️ Батч {batch_num}/{total_batches} ({progress:.0f}%)")
            
            # Валидируем батч с текущими параметрами
            current_params = params.copy()
            current_params['account_delay'] = current_delay
            
            batch_results = await self._validate_parallel_batch_optimized(batch, target_lang, current_params)
            
            # Обновляем результаты
            for key in ['validated', 'added', 'failed_validation', 'failed_db']:
                results[key] += batch_results[key]
            
            # Собираем добавленные аккаунты
            results['added_accounts'].extend(batch_results.get('added_accounts', []))
            
            # Адаптивное увеличение задержек для больших объемов
            if total_accounts > 1000 and batch_num % 10 == 0:
                current_delay *= 1.1  # Увеличиваем задержку на 10% каждые 10 батчей
                logger.info(f"🐌 Увеличена задержка до {current_delay:.1f}с для стабильности")
            
            # Пауза между батчами
            if i + batch_size < total_accounts:
                pause_time = params['batch_pause']
                # Для очень больших объемов увеличиваем паузы
                if total_accounts > 5000:
                    pause_time *= 1.5
                
                logger.info(f"⏳ Пауза {pause_time:.0f} сек между батчами...")
                await asyncio.sleep(pause_time)
        
        # СОЗДАЕМ ЗАДАЧИ ПОДПИСКИ для всех добавленных аккаунтов
        if results['added_accounts']:
            if progress_callback:
                await progress_callback("📺 Создаю задачи подписки для новых аккаунтов...")
            
            subscription_stats = await self._create_subscription_tasks_for_new_accounts(
                results['added_accounts'], target_lang
            )
            
            logger.info(f"📺 Создано {subscription_stats['tasks_created']} задач подписки")
        
        return results

    async def _validate_parallel_batch_optimized(self, batch: List[Dict], target_lang: str, params: Dict) -> Dict[str, int]:
        """
        Оптимизированная параллельная валидация с настраиваемыми параметрами
        """
        results = {
            'validated': 0, 
            'added': 0, 
            'failed_validation': 0, 
            'failed_db': 0,
            'added_accounts': []  # Для отслеживания добавленных аккаунтов
        }
        
        # Создаем задачи для параллельной валидации
        validation_tasks = []
        
        for idx, account_data in enumerate(batch):
            # Задержка перед запуском каждой задачи
            start_delay = idx * params['account_delay']
            
            task = asyncio.create_task(
                self._validate_and_add_single_optimized(account_data, target_lang, start_delay, params)
            )
            validation_tasks.append(task)
        
        # Ждем завершения всех задач в батче
        batch_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        # Анализируем результаты
        for idx, result in enumerate(batch_results):
            phone = batch[idx]['phone']
            
            if isinstance(result, dict) and result.get('success'):
                results['validated'] += 1
                results['added'] += 1
                
                # Сохраняем данные добавленного аккаунта
                if 'account_data' in result:
                    results['added_accounts'].append(result['account_data'])
                    
                logger.debug(f"✅ {phone}")
            elif isinstance(result, Exception):
                results['failed_validation'] += 1
                logger.debug(f"❌ {phone}: {str(result)[:50]}")
            else:
                results['failed_db'] += 1
                logger.debug(f"🚫 {phone}: DB error")
        
        return results

    async def _validate_and_add_single_optimized(self, account_data: Dict, target_lang: str, 
                                               delay: float, params: Dict) -> Dict:
        """
        Оптимизированная валидация одного аккаунта с адаптивными параметрами
        """
        # Ждем перед началом обработки
        if delay > 0:
            await asyncio.sleep(delay)
        
        phone = account_data['phone']
        
        try:
            # Валидируем аккаунт с параметрами под режим
            session_data = await self._validate_single_account_adaptive(account_data, params)
            
            # Добавляем в БД
            success = await add_account(
                phone=phone,
                session_data=session_data,
                lang=find_english_word(target_lang)
            )
            
            if success:
                # Создаем структуру аккаунта для дальнейшего использования
                account_dict = {
                    'phone_number': phone,
                    'session_data': session_data,
                    'lang': find_english_word(target_lang)
                }
                
                # Добавляем в пул сессий если загружен
                if global_session_manager.loading_complete:
                    await global_session_manager.add_new_session(account_dict)
                
                return {
                    'success': True, 
                    'phone': phone, 
                    'account_data': account_dict  # Возвращаем данные аккаунта
                }
            else:
                return {'success': False, 'phone': phone, 'error': 'DB_ERROR'}
                
        except Exception as e:
            return {'success': False, 'phone': phone, 'error': str(e)}

    async def _validate_single_account_adaptive(self, account_data: Dict, params: Dict) -> str:
        """
        Адаптивная валидация аккаунта с параметрами под конкретный режим
        """
        phone = account_data['phone']
        tdata_path = account_data['tdata_path']
        
        retry_count = params['retry_count']
        timeout = params['timeout']
        
        for attempt in range(retry_count):
            temp_session_path = None
            client = None
            
            try:
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
                
                # Создаем клиент с адаптивными параметрами
                client = TelegramClient(
                    session=str(temp_session_path),
                    api_id=API_ID,
                    api_hash=API_HASH,
                    connection_retries=1 if params['mode'] == 'instant' else 2,
                    timeout=timeout
                )
                
                await client.connect()
                
                # Проверяем авторизацию
                if not await client.is_user_authorized():
                    raise AccountValidationError(f"Session not authorized for {phone}")
                
                # Получаем session_data
                session_data = StringSession.save(client.session)
                await client.disconnect()
                
                logger.debug(f"✅ {phone} валидирован в режиме {params['mode']} (попытка {attempt + 1})")
                return session_data
                
            except Exception as e:
                if client:
                    try:
                        await client.disconnect()
                    except:
                        pass
                
                if temp_session_path and temp_session_path.exists():
                    try:
                        temp_session_path.unlink()
                    except:
                        pass
                
                if attempt == retry_count - 1:
                    raise AccountValidationError(f"Adaptive validation failed for {phone}: {e}")
                
                # Адаптивная пауза перед retry
                retry_delay = 1 if params['mode'] == 'instant' else (2 ** attempt)
                await asyncio.sleep(retry_delay)
                    
            finally:
                if temp_session_path and temp_session_path.exists():
                    try:
                        temp_session_path.unlink()
                    except:
                        pass
        
        raise AccountValidationError(f"Max adaptive validation retries exceeded for {phone}")
    
    async def _create_subscription_tasks_for_new_accounts(self, new_accounts: List[Dict], target_lang: str) -> Dict[str, int]:
        """
        Создает задачи подписки для новых аккаунтов на все существующие каналы языка
        """
        subscription_stats = {
            'channels_found': 0,
            'tasks_created': 0,
            'accounts_processed': 0
        }
        
        try:
            # Получаем все каналы для этого языка
            from database import get_channels_by_lang
            channels = await get_channels_by_lang(target_lang)
            
            if not channels:
                logger.info(f"📺 Нет каналов для языка {target_lang}, пропускаю создание задач подписки")
                return subscription_stats
            
            subscription_stats['channels_found'] = len(channels)
            logger.info(f"📺 Найдено {len(channels)} каналов для языка {target_lang}")
            
            # Создаем задачи подписки для каждого канала
            for channel_name in channels:
                try:
                    # Создаем задачи подписки только для новых аккаунтов
                    results = await self._create_subscription_tasks_for_accounts(
                        channel_name, new_accounts, target_lang
                    )
                    
                    subscription_stats['tasks_created'] += results.get('total_tasks', 0)
                    
                    logger.info(f"✅ Канал @{channel_name}: создано {results.get('total_tasks', 0)} задач подписки")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка создания задач для канала @{channel_name}: {e}")
            
            subscription_stats['accounts_processed'] = len(new_accounts)
            
            logger.info(f"""
📊 Создание задач подписки завершено:
   📺 Каналов: {subscription_stats['channels_found']}
   📱 Аккаунтов: {subscription_stats['accounts_processed']}
   📋 Задач создано: {subscription_stats['tasks_created']}
            """)
            
            return subscription_stats
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач подписки: {e}")
            return subscription_stats

    async def _create_subscription_tasks_for_accounts(self, channel_name: str, accounts: List[Dict], target_lang: str) -> Dict[str, int]:
        """
        Создает задачи подписки для конкретных аккаунтов на один канал
        """
        results = {
            'total_tasks': 0,
            'accounts_processed': 0
        }
        
        try:
            if not accounts:
                return results
            
            # Импортируем необходимые модули
            from config import find_english_word, read_setting
            
            english_lang = find_english_word(target_lang)
            
            # Получаем параметры задержек подписок
            params = {
                'base_delay': read_setting('lag.txt', 30.0) * 60,      # минуты в секунды
                'range_val': read_setting('range.txt', 5.0) * 60,      # минуты в секунды  
                'accounts_delay': read_setting('accounts_delay.txt', 10.0) * 60,
                'timeout_count': int(read_setting('timeout_count.txt', 4.0)),
                'timeout_duration': read_setting('timeout_duration.txt', 20.0) * 60
            }
            
            logger.info(f"📅 Создаю задачи подписки для {len(accounts)} аккаунтов на @{channel_name}")
            
            # Перемешиваем аккаунты для равномерности
            random.shuffle(accounts)
            
            # Создаем задачи с рассчитанными задержками
            subscription_tasks = []
            current_time = time.time()
            
            for account_idx, account in enumerate(accounts):
                # Рассчитываем задержку для этого аккаунта
                delay_seconds = await self._calculate_subscription_delay_for_account(
                    account_idx, params
                )
                
                execute_at = current_time + delay_seconds
                
                task = TaskItem(
                    account_session=account['session_data'],
                    phone=account['phone_number'],
                    channel=channel_name,
                    lang=english_lang,
                    task_type=TaskType.SUBSCRIBE,
                    execute_at=execute_at
                )
                
                subscription_tasks.append(task)
            
            # Отправляем задачи в очередь
            await self._schedule_subscription_tasks_direct(subscription_tasks)
            
            results['total_tasks'] = len(subscription_tasks)
            results['accounts_processed'] = len(accounts)
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка создания задач подписки для аккаунтов: {e}")
            return results

    async def _calculate_subscription_delay_for_account(self, account_index: int, params: Dict[str, float]) -> float:
        """
        Рассчитывает задержку для подписки конкретного аккаунта
        """
        base_delay = params['base_delay']
        range_val = params['range_val']
        accounts_delay = params['accounts_delay']
        timeout_count = params['timeout_count']
        timeout_duration = params['timeout_duration']
        
        # 1. Базовая задержка между аккаунтами
        account_delay = account_index * accounts_delay
        
        # 2. Добавляем случайный разброс
        random_variation = random.uniform(-range_val, range_val)
        
        # 3. Добавляем паузы после каждых timeout_count подписок
        timeout_cycles = account_index // timeout_count
        timeout_delay = timeout_cycles * timeout_duration
        
        # 4. Итоговая задержка
        total_delay = account_delay + random_variation + timeout_delay
        
        # 5. Минимальная задержка - не менее базовой
        total_delay = max(total_delay, base_delay)
        
        return total_delay

    async def _schedule_subscription_tasks_direct(self, tasks):
        """
        Планирует задачи подписки напрямую в Redis
        """
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
            
            # Сортируем задачи по времени выполнения
            tasks.sort(key=lambda x: x.execute_at)
            
            for task in tasks:
                task_data = {
                    'account_session': task.account_session,
                    'phone': task.phone,
                    'channel': task.channel,
                    'lang': task.lang,
                    'task_type': task.task_type.value,
                    'execute_at': task.execute_at,
                    'retry_count': task.retry_count,
                    'created_at': time.time()
                }
                
                # Добавляем в очередь подписок
                redis_client.lpush("subscription_tasks", json.dumps(task_data))
            
            # TTL на 48 часов
            redis_client.expire("subscription_tasks", 48 * 3600)
            
            logger.info(f"📋 Запланировано {len(tasks)} задач подписки")
            
        except Exception as e:
            logger.error(f"Ошибка планирования задач подписки: {e}")
    
    # ========== СТАРЫЕ МЕТОДЫ ДЛЯ СОВМЕСТИМОСТИ ==========
    
    async def add_accounts_from_zip_with_validation(self, zip_path: Path, target_lang: str, 
                                                  progress_callback=None) -> Dict[str, int]:
        """
        Добавляет аккаунты из ZIP с ОБЯЗАТЕЛЬНОЙ валидацией (теперь адаптивной)
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
            
            added_accounts = []  # Для отслеживания добавленных аккаунтов
            
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
                            
                            # Сохраняем данные добавленного аккаунта
                            added_accounts.append({
                                'phone_number': phone,
                                'session_data': session_data,
                                'lang': find_english_word(target_lang)
                            })
                            
                            if (idx + 1) % 100 == 0:
                                logger.info(f"➕ Добавлено {idx + 1}/{len(new_accounts)} аккаунтов")
                        else:
                            results['failed_db'] += 1
                    else:
                        results['failed_db'] += 1
                        
                except Exception as e:
                    logger.error(f"Ошибка добавления {phone}: {e}")
                    results['failed_db'] += 1
            
            # 4. СОЗДАЕМ ЗАДАЧИ ПОДПИСКИ для добавленных аккаунтов
            if added_accounts:
                if progress_callback:
                    await progress_callback("📺 Создаю задачи подписки для новых аккаунтов...")
                
                subscription_stats = await self._create_subscription_tasks_for_new_accounts(
                    added_accounts, target_lang
                )
                
                logger.info(f"📺 Создано {subscription_stats['tasks_created']} задач подписки")
            
            # 5. Финальный отчет
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
    
    async def delete_accounts_by_status(self, status: str, limit: int = None) -> int:
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
            if status == 'all':
                accounts_to_delete = await get_all_accounts()
            else:
                accounts_to_delete = []
                all_accounts = await get_all_accounts()
                for account in all_accounts:
                    if account['status'] == status:
                        accounts_to_delete.append(account)
            
            if limit:
                accounts_to_delete = accounts_to_delete[:limit]
            
            if not accounts_to_delete:
                logger.info(f"❌ Нет аккаунтов со статусом '{status}' для удаления")
                return 0
            
            # Удаляем из пула сессий
            for account in accounts_to_delete:
                await global_session_manager.remove_session(account['session_data'])
            
            # Удаляем из БД
            from database import delete_accounts_by_status as db_delete_accounts_by_status
            deleted_count = await db_delete_accounts_by_status(status, limit)
            
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