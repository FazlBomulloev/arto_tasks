import asyncio
import logging
import time
import random
import math
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from config import (
    VIEW_TASK_DURATION, BATCH_SIZE, read_setting, 
    find_english_word, find_lang_code
)
from database import get_accounts_by_lang, get_channels_by_lang
from session_manager import global_session_manager
from exceptions import TaskProcessingError

logger = logging.getLogger(__name__)

class TaskType(Enum):
    VIEW = "view"
    SUBSCRIBE = "subscribe"

@dataclass
class TaskItem:
    account_session: str
    phone: str
    channel: str  
    lang: str
    task_type: TaskType
    post_id: Optional[int] = None
    execute_at: Optional[float] = None
    retry_count: int = 0

class TaskService:
    """Сервис для управления задачами с батчингом"""
    
    def __init__(self):
        self.batch_size = BATCH_SIZE
        self.view_duration = VIEW_TASK_DURATION
        
    async def create_view_tasks_for_post(self, channel_username: str, post_id: int) -> Dict[str, int]:
        """
        Создает задачи просмотра для поста с распределением на 10 часов
        
        Returns:
            Dict с результатами создания задач
        """
        results = {
            'total_tasks': 0,
            'batches_created': 0,
            'languages': 0
        }
        
        try:
            # 1. Получаем языки канала из БД
            languages = await self._get_channel_languages(channel_username)
            if not languages:
                logger.error(f"⛔ Канал @{channel_username} не найден в БД")
                return results
            
            results['languages'] = len(languages)
            all_tasks = []
            
            # 2. Для каждого языка получаем аккаунты
            for lang in languages:
                english_lang = find_english_word(lang)
                accounts = await get_accounts_by_lang(english_lang, 'active')
                
                if not accounts:
                    logger.warning(f"⚠ Нет активных аккаунтов для языка {lang}")
                    continue
                
                # Создаем задачи для аккаунтов этого языка
                for account in accounts:
                    task = TaskItem(
                        account_session=account['session_data'],
                        phone=account['phone_number'],
                        channel=channel_username,
                        lang=english_lang,
                        task_type=TaskType.VIEW,
                        post_id=post_id
                    )
                    all_tasks.append(task)
            
            results['total_tasks'] = len(all_tasks)
            
            if not all_tasks:
                logger.warning("⚠ Нет задач для создания")
                return results
            
            # 3. Создаем отложенные батчи на 10 часов
            batches = await self._create_delayed_view_batches(all_tasks, post_id)
            results['batches_created'] = len(batches)
            
            logger.info(f"""
📊 Создано задач просмотра для поста {post_id}:
   📱 Всего задач: {results['total_tasks']}
   🌐 Языков: {results['languages']}  
   📦 Батчей: {results['batches_created']}
   ⏰ Распределение: 10 часов
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач просмотра: {e}")
            raise TaskProcessingError(f"Failed to create view tasks: {e}")
    
    async def _get_channel_languages(self, channel_username: str) -> List[str]:
        """Получает языки канала из БД"""
        try:
            from database import db_session
            async with db_session() as conn:
                results = await conn.fetch(
                    'SELECT DISTINCT lang FROM channels WHERE name = $1',
                    channel_username
                )
                return [result['lang'] for result in results]
        except Exception as e:
            logger.error(f"Ошибка получения языков канала: {e}")
            return []
    
    async def _create_delayed_view_batches(self, all_tasks: List[TaskItem], post_id: int) -> List[Dict]:
        """Создает отложенные батчи с равномерным распределением на 10 часов"""
        
        if not all_tasks:
            return []
        
        # Перемешиваем задачи для равномерности
        random.shuffle(all_tasks)
        
        total_accounts = len(all_tasks)
        duration_seconds = self.view_duration  # 10 часов в секундах
        
        logger.info(f"📊 Распределение {total_accounts} аккаунтов на {duration_seconds/3600:.1f} часов")
        
        # Рассчитываем интервал между ОТДЕЛЬНЫМИ аккаунтами
        if total_accounts > 1:
            interval_per_account = duration_seconds / (total_accounts - 1)
        else:
            interval_per_account = 0
        
        logger.info(f"⏱️ Интервал между аккаунтами: {interval_per_account/60:.1f} минут")
        
        # Создаем задачи с индивидуальным временем выполнения
        current_time = time.time()
        batches = []
        
        # Группируем по 1 задаче (каждый аккаунт = отдельный батч)
        for idx, task in enumerate(all_tasks):
            # Время выполнения для этого аккаунта
            execute_at = current_time + (idx * interval_per_account)
            
            # Добавляем небольшую рандомизацию (±2 минуты)
            randomization = random.uniform(-120, 120)  # ±2 минуты
            execute_at += randomization
            
            # Устанавливаем время выполнения
            task.execute_at = execute_at
            
            batch_info = {
                'batch_number': idx + 1,
                'tasks': [task],  # Один аккаунт на батч
                'execute_at': execute_at,
                'delay_minutes': (execute_at - current_time) / 60,
                'account_phone': task.phone
            }
            
            batches.append(batch_info)
            
            # Логируем каждый 100-й батч для отслеживания
            if (idx + 1) % 100 == 0 or idx == len(all_tasks) - 1:
                logger.info(f"📦 Батч {idx + 1}/{total_accounts}: @{task.phone} через {batch_info['delay_minutes']:.1f} мин")
        
        # Отправляем батчи в очередь обработки
        await self._schedule_batches(batches, post_id)
        
        logger.info(f"""
✅ Создано {len(batches)} индивидуальных батчей:
   ⏰ Первый аккаунт: сейчас
   ⏰ Последний аккаунт: через {duration_seconds/3600:.1f} часов
   📊 Интервал: {interval_per_account/60:.2f} минут между аккаунтами
        """)
        
        return batches
    
    async def _schedule_batches(self, batches: List[Dict], post_id: int):
        """Планирует выполнение батчей через Redis/FastStream"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT, 
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            for batch_info in batches:
                # Сериализуем данные батча
                batch_data = {
                    'batch_number': batch_info['batch_number'],
                    'execute_at': batch_info['execute_at'],
                    'post_id': post_id,
                    'tasks': [
                        {
                            'account_session': task.account_session,
                            'phone': task.phone,
                            'channel': task.channel,
                            'lang': task.lang,
                            'post_id': task.post_id,
                            'execute_at': task.execute_at
                        }
                        for task in batch_info['tasks']
                    ]
                }
                
                # Добавляем в очередь Redis
                queue_key = f"delayed_view_batches"
                redis_client.lpush(queue_key, str(batch_data))
                
                # Устанавливаем TTL на 12 часов (с запасом)
                redis_client.expire(queue_key, 12 * 3600)
            
            logger.info(f"📋 Запланировано {len(batches)} батчей в Redis")
            
        except Exception as e:
            logger.error(f"Ошибка планирования батчей: {e}")
            raise TaskProcessingError(f"Failed to schedule batches: {e}")
    
    async def create_subscription_tasks(self, channel_name: str, target_lang: str) -> Dict[str, int]:
        """
        Создает задачи подписки с умными задержками (как раньше)
        
        Returns:
            Dict с результатами создания задач
        """
        results = {
            'total_tasks': 0,
            'accounts_processed': 0
        }
        
        try:
            # Получаем аккаунты для подписки
            english_lang = find_english_word(target_lang)
            accounts = await get_accounts_by_lang(english_lang, 'active')
            
            if not accounts:
                logger.warning(f"⚠ Нет активных аккаунтов для языка {target_lang}")
                return results
            
            results['accounts_processed'] = len(accounts)
            
            # Получаем параметры задержек
            params = await self._get_subscription_delays()
            
            # Создаем задачи с рассчитанными задержками
            subscription_tasks = []
            
            for account_idx, account in enumerate(accounts):
                # Рассчитываем задержку для этого аккаунта
                delay = await self._calculate_subscription_delay(
                    account_idx, 0, params  # subscription_index = 0 для нового канала
                )
                
                task = TaskItem(
                    account_session=account['session_data'],
                    phone=account['phone_number'],
                    channel=channel_name,
                    lang=english_lang,
                    task_type=TaskType.SUBSCRIBE,
                    execute_at=time.time() + delay
                )
                
                subscription_tasks.append(task)
            
            results['total_tasks'] = len(subscription_tasks)
            
            # Отправляем задачи в очередь
            await self._schedule_subscription_tasks(subscription_tasks)
            
            logger.info(f"""
📊 Создано задач подписки на канал @{channel_name}:
   📱 Аккаунтов: {results['accounts_processed']}
   📋 Задач: {results['total_tasks']}
   🌐 Язык: {target_lang}
            """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач подписки: {e}")
            raise TaskProcessingError(f"Failed to create subscription tasks: {e}")
    
    async def _get_subscription_delays(self) -> Dict[str, float]:
        """Получает параметры задержек подписок из файлов"""
        return {
            'base_delay': read_setting('lag.txt', 30.0) * 60,      # минуты в секунды
            'range_val': read_setting('range.txt', 5.0) * 60,      # минуты в секунды  
            'accounts_delay': read_setting('accounts_delay.txt', 10.0) * 60,
            'timeout_count': int(read_setting('timeout_count.txt', 4.0)),
            'timeout_duration': read_setting('timeout_duration.txt', 20.0) * 60
        }
    
    async def _calculate_subscription_delay(self, account_index: int, 
                                          subscription_index: int, 
                                          params: Dict[str, float]) -> float:
        """Рассчитывает задержку для подписки (логика как в оригинале)"""
        
        # Базовые параметры
        base_delay = params['base_delay']
        range_val = params['range_val'] 
        accounts_delay = params['accounts_delay']
        timeout_count = params['timeout_count']
        timeout_duration = params['timeout_duration']
        
        # 1. Задержка между аккаунтами
        account_delay = account_index * accounts_delay * random.uniform(0.9, 1.1)
        
        # 2. Задержка между подписками  
        subscription_delay = subscription_index * base_delay * random.uniform(0.9, 1.1)
        
        # 3. Таймауты после определенного количества подписок
        full_timeouts = subscription_index // timeout_count
        timeout_delay = full_timeouts * timeout_duration * random.uniform(0.9, 1.1)
        
        # Общая задержка
        total_delay = account_delay + subscription_delay + timeout_delay
        
        # 4. Добавляем случайную вариацию
        variation = random.uniform(-range_val, range_val)
        total_delay = max(0, total_delay + variation)
        
        return total_delay
    
    async def _schedule_subscription_tasks(self, tasks: List[TaskItem]):
        """Планирует задачи подписки"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD, 
                decode_responses=True
            )
            
            for task in tasks:
                task_data = {
                    'account_session': task.account_session,
                    'phone': task.phone,
                    'channel': task.channel,
                    'lang': task.lang,
                    'task_type': task.task_type.value,
                    'execute_at': task.execute_at,
                    'retry_count': task.retry_count
                }
                
                # Добавляем в очередь подписок
                redis_client.lpush("subscription_tasks", str(task_data))
                
                # TTL на 24 часа
                redis_client.expire("subscription_tasks", 24 * 3600)
            
            logger.info(f"📋 Запланировано {len(tasks)} задач подписки")
            
        except Exception as e:
            logger.error(f"Ошибка планирования задач подписки: {e}")
            raise TaskProcessingError(f"Failed to schedule subscription tasks: {e}")
    
    async def get_task_stats(self) -> Dict[str, int]:
        """Получает статистику задач из Redis"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            stats = {
                'pending_view_batches': redis_client.llen("delayed_view_batches"),
                'pending_subscriptions': redis_client.llen("subscription_tasks"),
                'retry_queue': redis_client.llen("retry_tasks"),
                'session_pool_size': len(global_session_manager.clients),
                'session_pool_loaded': global_session_manager.loading_complete
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики задач: {e}")
            return {}

# Глобальный экземпляр сервиса
task_service = TaskService()
