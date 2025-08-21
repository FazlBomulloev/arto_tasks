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
        """
        Создает задачи просмотра для нового поста
        """
        results = {
            'total_tasks': 0,
            'languages': 0
        }
        
        try:
            view_duration = self.get_view_duration()
            view_hours = view_duration / 3600
            
            logger.info(f"📊 Создание задач просмотра: {view_hours} часов для @{channel_username}")
            
            # 1. Получаем языки канала из БД
            languages = await self._get_channel_languages(channel_username)
            if not languages:
                logger.error(f"⛔ Канал @{channel_username} не найден в БД")
                return results
            
            results['languages'] = len(languages)
            all_tasks = []
            
            # 2. Собираем все аккаунты всех языков из БД
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
            
            # 3. Равномерно распределяем по времени и сохраняем в Redis
            await self._schedule_view_tasks_simple(all_tasks, view_duration)
            
            logger.info(f"""
✅ Создано {results['total_tasks']} задач просмотра:
   📺 Пост: {post_id}
   🌐 Языков: {results['languages']}  
   ⏰ Период: {view_hours} часов""")
            
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
    
    async def _schedule_view_tasks_simple(self, tasks: List[TaskItem], duration_seconds: int):
        """Планирует задачи просмотра в единую Redis очередь"""
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
                interval = min(interval, 120)  # Максимум 2 минуты
            else:
                interval = 0
            
            logger.info(f"⏱️ Интервал между просмотрами: {interval:.1f} секунд")
            
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
            
            # Записываем все задачи в единую sorted set
            if tasks_data:
                self.redis_client.zadd("task_queue", tasks_data)
                
                # TTL на 48 часов
                self.redis_client.expire("task_queue", 48 * 3600)
                
                first_time = min(tasks_data.values())
                last_time = max(tasks_data.values())
                
                logger.info(f"""
📋 Добавлено {len(tasks)} задач просмотра в task_queue:
   ⏰ Первая задача: через {(first_time - current_time)/60:.1f} мин
   ⏰ Последняя задача: через {(last_time - current_time)/60:.1f} мин
   📊 Период: {(last_time - first_time)/3600:.2f} часов
                """)
            
        except Exception as e:
            logger.error(f"Ошибка планирования задач просмотра: {e}")
            raise TaskProcessingError(f"Failed to schedule view tasks: {e}")
    
    async def create_subscription_tasks(self, channel_name: str, target_lang: str) -> Dict[str, int]:
        results = {
            'total_tasks': 0,
            'accounts_processed': 0
        }
        
        try:
            # Получаем аккаунты языка из БД
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
   ⏸️ Длительность паузы: {timeout_duration/60:.1f} мин""")
            
            # Перемешиваем аккаунты для равномерности
            random.shuffle(accounts)
            
            # Создаем задачи с правильной последовательной логикой
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
   📊 Общая длительность: {duration_hours:.1f} часов""")
            
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
    
    async def create_subscription_tasks_for_new_accounts(self, accounts: List[Dict], target_lang: str) -> Dict[str, int]:
        """
        Создает задачи подписки для новых аккаунтов на все каналы языка
        Используется при добавлении новых аккаунтов
        """
        subscription_stats = {
            'channels_found': 0,
            'tasks_created': 0,
            'accounts_processed': 0
        }
        
        try:
            # Получаем все каналы языка из БД
            channels = await get_channels_by_lang(target_lang)
            
            if not channels:
                logger.info(f"📺 Нет каналов для языка {target_lang}")
                return subscription_stats
            
            subscription_stats['channels_found'] = len(channels)
            subscription_stats['accounts_processed'] = len(accounts)
            
            logger.info(f"📺 Создание задач подписки: {len(accounts)} аккаунтов на {len(channels)} каналов")
            
            # Создаем задачи для каждого канала
            total_tasks_created = 0
            
            for channel_name in channels:
                try:
                    channel_tasks = await self._create_subscription_tasks_for_channel(
                        channel_name, accounts, target_lang
                    )
                    
                    total_tasks_created += channel_tasks
                    logger.debug(f"✅ Канал @{channel_name}: {channel_tasks} задач")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка создания задач для @{channel_name}: {e}")
            
            subscription_stats['tasks_created'] = total_tasks_created
            
            logger.info(f"📊 Создано {total_tasks_created} задач подписки для новых аккаунтов")
            return subscription_stats
            
        except Exception as e:
            logger.error(f"💥 Ошибка создания задач подписки для новых аккаунтов: {e}")
            return subscription_stats
    
    async def _create_subscription_tasks_for_channel(self, channel_name: str, accounts: List[Dict], target_lang: str) -> int:
        """Создает задачи подписки на один канал для списка аккаунтов"""
        try:
            if not accounts:
                return 0
            
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
            # Подготавливаем данные для Redis
            tasks_data = {}
            
            for task in tasks:
                task_json = json.dumps(task)
                execute_at = task['execute_at']
                tasks_data[task_json] = execute_at
            
            # Сохраняем в единую очередь
            if tasks_data:
                self.redis_client.zadd("task_queue", tasks_data)
                self.redis_client.expire("task_queue", 48 * 3600)  # TTL 48 часов
                
                logger.debug(f"📋 Сохранено {len(tasks)} задач в Redis")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения задач в Redis: {e}")
    
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
            
            # Анализ типов задач (для диагностики)
            sample_tasks = self.redis_client.zrangebyscore(
                "task_queue", 0, '+inf', start=0, num=100
            )
            
            view_count = 0
            subscribe_count = 0
            
            for task_json in sample_tasks:
                try:
                    task_data = json.loads(task_json)
                    task_type = task_data.get('task_type', '')
                    if task_type == 'view':
                        view_count += 1
                    elif task_type == 'subscribe':
                        subscribe_count += 1
                except:
                    pass
            
            return {
                'total_tasks': total_tasks,
                'ready_tasks': ready_tasks,
                'future_tasks': future_tasks,
                'retry_tasks': retry_tasks,
                'queue_name': 'task_queue',
                'sample_views': view_count,
                'sample_subscribes': subscribe_count
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики задач: {e}")
            return {}
    
    async def get_detailed_task_analysis(self) -> Dict:
        """Получает детальный анализ задач для диагностики"""
        try:
            current_time = time.time()
            
            # Получаем образец задач для анализа
            sample_tasks = self.redis_client.zrangebyscore(
                "task_queue", 
                min=0, 
                max='+inf',
                withscores=True,
                start=0, 
                num=500
            )
            
            analysis = {
                'total_analyzed': len(sample_tasks),
                'by_type': {'view': 0, 'subscribe': 0, 'unknown': 0},
                'by_timing': {
                    'ready_now': 0,
                    'ready_5min': 0,
                    'ready_1hour': 0,
                    'future': 0
                },
                'by_language': {},
                'oldest_task': None,
                'newest_task': None
            }
            
            for task_json, execute_at in sample_tasks:
                try:
                    task_data = json.loads(task_json)
                    
                    # Анализ по типам
                    task_type = task_data.get('task_type', 'unknown')
                    analysis['by_type'][task_type] = analysis['by_type'].get(task_type, 0) + 1
                    
                    # Анализ по времени
                    time_diff = execute_at - current_time
                    if time_diff <= 0:
                        analysis['by_timing']['ready_now'] += 1
                    elif time_diff <= 300:  # 5 минут
                        analysis['by_timing']['ready_5min'] += 1
                    elif time_diff <= 3600:  # 1 час
                        analysis['by_timing']['ready_1hour'] += 1
                    else:
                        analysis['by_timing']['future'] += 1
                    
                    # Анализ по языкам
                    lang = task_data.get('lang', 'unknown')
                    analysis['by_language'][lang] = analysis['by_language'].get(lang, 0) + 1
                    
                    # Самые старые и новые задачи
                    created_at = task_data.get('created_at', execute_at)
                    if analysis['oldest_task'] is None or created_at < analysis['oldest_task']:
                        analysis['oldest_task'] = created_at
                    if analysis['newest_task'] is None or created_at > analysis['newest_task']:
                        analysis['newest_task'] = created_at
                        
                except Exception as e:
                    logger.debug(f"Ошибка анализа задачи: {e}")
                    continue
            
            # Конвертируем timestamps
            if analysis['oldest_task']:
                analysis['oldest_task'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(analysis['oldest_task']))
            if analysis['newest_task']:
                analysis['newest_task'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(analysis['newest_task']))
            
            return analysis
            
        except Exception as e:
            logger.error(f"Ошибка детального анализа задач: {e}")
            return {}
    
    async def cleanup_expired_tasks(self, max_age_hours: float = 48.0) -> int:
        """Очищает просроченные задачи"""
        try:
            cutoff_time = time.time() - (max_age_hours * 3600)
            
            # Получаем просроченные задачи
            expired_tasks = self.redis_client.zrangebyscore(
                "task_queue",
                min=0,
                max=cutoff_time,
                start=0,
                num=1000
            )
            
            cleaned_count = 0
            if expired_tasks:
                # Удаляем просроченные задачи
                for task_json in expired_tasks:
                    self.redis_client.zrem("task_queue", task_json)
                    cleaned_count += 1
                
                logger.info(f"🗑️ Очищено {cleaned_count} просроченных задач (>{max_age_hours}ч)")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Ошибка очистки просроченных задач: {e}")
            return 0

# Глобальный экземпляр 
task_service = TaskService()