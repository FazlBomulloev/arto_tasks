import asyncio
import logging
import time
import random
import json
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from config import read_setting, find_english_word
from database import get_accounts_by_lang, get_channels_by_lang
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
    def __init__(self):
        self.redis_client = None
        self._init_redis()
        
    def _init_redis(self):
        """Инициализация Redis"""
        try:
            from redis import Redis
            from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
            
            self.redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
        
    def get_view_duration(self) -> int:
        """Получает длительность просмотров из настроек"""
        hours = read_setting('followPeriod.txt', 3.0)
        return int(hours * 3600)
        
    async def create_view_tasks_for_post(self, channel_username: str, post_id: int) -> Dict[str, int]:
        results = {
            'total_tasks': 0,
            'languages': 0
        }
        
        try:
            view_duration = self.get_view_duration()
            view_hours = view_duration / 3600
            
            logger.info(f"📊 Создание задач просмотра: {view_hours} часов для @{channel_username}")
            
            # 1. Получаем языки канала
            languages = await self._get_channel_languages(channel_username)
            if not languages:
                logger.error(f"⛔ Канал @{channel_username} не найден в БД")
                return results
            
            results['languages'] = len(languages)
            all_tasks = []
            
            # 2. Собираем все аккаунты всех языков
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
            
            # 3. Равномерно распределяем по времени и добавляем в одну очередь
            await self._schedule_tasks_simple(all_tasks, view_duration)
            
            logger.info(f"""
✅ Создано {results['total_tasks']} задач просмотра:
   📺 Пост: {post_id}
   🌐 Языков: {results['languages']}  
   ⏰ Период: {view_hours} часов
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
    
    async def _schedule_tasks_simple(self, tasks: List[TaskItem], duration_seconds: int):
        """Планирует все задачи в одну Redis очередь"""
        if not tasks:
            return
        
        try:
            # Перемешиваем для равномерности
            random.shuffle(tasks)
            current_time = time.time()
            
            # Рассчитываем равномерные интервалы
            if len(tasks) > 1:
                interval = duration_seconds / len(tasks)
                interval = max(interval, 30)  # Минимум 30 секунд
                interval = min(interval, 90)
            else:
                interval = 0
            
            logger.info(f"⏱️ Интервал между задачами: {interval:.1f} секунд")
            
            # Подготавливаем данные для Redis
            tasks_data = {}
            
            for idx, task in enumerate(tasks):
                execute_at = current_time + (idx * interval)
                
                # Добавляем небольшую рандомизацию
                randomization = random.uniform(-interval * 0.1, interval * 0.1)
                execute_at += randomization
                execute_at = max(execute_at, current_time + 10)  # Минимум через 10 сек
                
                task.execute_at = execute_at
                
                task_data = {
                    'account_session': task.account_session,
                    'phone': task.phone,
                    'channel': task.channel,
                    'lang': task.lang,
                    'task_type': task.task_type.value,
                    'post_id': task.post_id,
                    'execute_at': execute_at,
                    'retry_count': task.retry_count,
                    'created_at': time.time()
                }
                
                # Используем execute_at как score для сортировки
                tasks_data[json.dumps(task_data)] = execute_at
            
            # Записываем все задачи в одну sorted set
            if tasks_data:
                self.redis_client.zadd("task_queue", tasks_data)
                
                # TTL на 48 часов
                self.redis_client.expire("task_queue", 48 * 3600)
                
                first_time = min(tasks_data.values())
                last_time = max(tasks_data.values())
                
                logger.info(f"""
📋 Добавлено {len(tasks)} задач в task_queue:
   ⏰ Первая задача: через {(first_time - current_time)/60:.1f} мин
   ⏰ Последняя задача: через {(last_time - current_time)/60:.1f} мин
   📊 Период: {(last_time - first_time)/3600:.2f} часов
                """)
            
        except Exception as e:
            logger.error(f"Ошибка планирования задач: {e}")
            raise TaskProcessingError(f"Failed to schedule tasks: {e}")
    
    async def create_subscription_tasks(self, channel_name: str, target_lang: str) -> Dict[str, int]:
        """Создает задачи подписки с правильной последовательной логикой"""
        results = {
            'total_tasks': 0,
            'accounts_processed': 0
        }
        
        try:
            # Получаем аккаунты
            english_lang = find_english_word(target_lang)
            accounts = await get_accounts_by_lang(english_lang, 'active')
            
            if not accounts:
                logger.warning(f"⚠ Нет активных аккаунтов для языка {target_lang}")
                return results
            
            results['accounts_processed'] = len(accounts)
            
            # Получаем параметры задержек из настроек
            base_delay = read_setting('lag.txt', 14.0) * 60  # в секундах
            range_val = read_setting('range.txt', 5.0) * 60  # в секундах
            timeout_count = int(read_setting('timeout_count.txt', 3.0))
            timeout_duration = read_setting('timeout_duration.txt', 13.0) * 60  # в секундах
            
            logger.info(f"""
📺 СОЗДАНИЕ ЗАДАЧ ПОДПИСКИ ДЛЯ @{channel_name}:
   📱 Аккаунтов: {len(accounts)}
   🌐 Язык: {target_lang}
   ⏰ Базовая задержка: {base_delay/60:.1f} мин
   🎲 Разброс: ±{range_val/60:.1f} мин
   🔢 Подписок до паузы: {timeout_count}
   ⏸️ Длительность паузы: {timeout_duration/60:.1f} мин
            """)
            
            # Перемешиваем аккаунты для равномерности
            random.shuffle(accounts)
            
            # Создаем задачи с ПРАВИЛЬНОЙ последовательной логикой
            subscription_tasks = []
            current_time = time.time()
            
            # Время выполнения для первого аккаунта = сразу
            next_execute_time = current_time
            
            for account_idx, account in enumerate(accounts):
                # Рассчитываем execute_at для текущего аккаунта
                if account_idx == 0:
                    # Первый аккаунт - сразу
                    execute_at = next_execute_time
                else:
                    # Проверяем нужна ли дополнительная пауза
                    if account_idx % timeout_count == 0:
                        # После каждых timeout_count подписок добавляем паузу
                        pause_delay = timeout_duration
                        logger.debug(f"⏸️ Пауза {timeout_duration/60:.1f} мин после {account_idx} подписок")
                    else:
                        pause_delay = 0
                    
                    # Базовая задержка + разброс + возможная пауза
                    random_variation = random.uniform(-range_val, range_val)
                    total_delay = base_delay + random_variation + pause_delay
                    
                    execute_at = next_execute_time + total_delay
                
                # Обновляем время для следующего аккаунта
                next_execute_time = execute_at
                
                task = TaskItem(
                    account_session=account['session_data'],
                    phone=account['phone_number'],
                    channel=channel_name,
                    lang=english_lang,
                    task_type=TaskType.SUBSCRIBE,
                    execute_at=execute_at
                )
                
                subscription_tasks.append(task)
                
                # Логируем для отладки первых 5 задач
                if account_idx < 5:
                    delay_from_start = (execute_at - current_time) / 60
                    logger.debug(f"📋 {account['phone_number']}: через {delay_from_start:.1f} мин")
            
            results['total_tasks'] = len(subscription_tasks)
            
            # Планируем в ту же очередь что и просмотры
            await self._schedule_subscription_tasks_simple(subscription_tasks)
            
            # Статистика времени
            if subscription_tasks:
                first_time = min(task.execute_at for task in subscription_tasks)
                last_time = max(task.execute_at for task in subscription_tasks)
                duration_hours = (last_time - first_time) / 3600
                
                logger.info(f"""
✅ Создано {results['total_tasks']} задач подписки:
   📺 Канал: @{channel_name}
   📱 Аккаунтов: {results['accounts_processed']}
   ⏰ Первая подписка: сразу
   🕐 Последняя подписка: через {duration_hours:.1f} часов
   📊 Общая длительность: {duration_hours:.1f} часов
                """)
            
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач подписки: {e}")
            raise TaskProcessingError(f"Failed to create subscription tasks: {e}")
    
    async def _schedule_subscription_tasks_simple(self, tasks: List[TaskItem]):
        """Планирует задачи подписки в общую очередь"""
        try:
            tasks_data = {}
            
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
                
                tasks_data[json.dumps(task_data)] = task.execute_at
            
            # Добавляем в ту же очередь что и просмотры
            if tasks_data:
                self.redis_client.zadd("task_queue", tasks_data)
                
                # TTL на 48 часов
                self.redis_client.expire("task_queue", 48 * 3600)
                
                logger.info(f"📋 Добавлено {len(tasks)} задач подписки в task_queue")
            
        except Exception as e:
            logger.error(f"Ошибка планирования задач подписки: {e}")
            raise TaskProcessingError(f"Failed to schedule subscription tasks: {e}")
    
    async def get_task_stats(self) -> Dict[str, int]:
        """Получает статистику задач из общей очереди"""
        try:
            current_time = time.time()
            
            # Общее количество задач
            total_tasks = self.redis_client.zcard("task_queue") or 0
            
            # Готовые к выполнению
            ready_tasks = self.redis_client.zcount("task_queue", 0, current_time) or 0
            
            # Будущие задачи
            future_tasks = total_tasks - ready_tasks
            
            # Retry задачи
            retry_tasks = self.redis_client.llen("retry_tasks") or 0
            
            return {
                'total_tasks': total_tasks,
                'ready_tasks': ready_tasks,
                'future_tasks': future_tasks,
                'retry_tasks': retry_tasks,
                'queue_name': 'task_queue'
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики задач: {e}")
            return {}

# Глобальный экземпляр 
task_service = TaskService()