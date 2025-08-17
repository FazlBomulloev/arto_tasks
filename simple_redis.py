#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с шардами в Redis
Запустите его чтобы увидеть что происходит с очередями
"""

import json
import time
from redis import Redis

# Настройки Redis
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6389
REDIS_PASSWORD = 'mybotredis2024'  # Из vars/password.txt

def main():
    print("🔍 ДИАГНОСТИКА REDIS ШАРДОВ")
    print("=" * 50)
    
    try:
        # Подключение к Redis
        redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        
        # Проверка подключения
        redis_client.ping()
        print("✅ Подключение к Redis успешно")
        
        current_time = time.time()
        print(f"🕐 Текущее время: {current_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))})")
        
        # ========== ПРОВЕРКА АКТИВНЫХ ШАРДОВ ==========
        print("\n📊 АКТИВНЫЕ ШАРДЫ:")
        
        # Шарды просмотров
        view_shards_count = redis_client.zcard("active_view_shards")
        print(f"   👀 Шардов просмотров: {view_shards_count}")
        
        # Шарды подписок
        sub_shards_count = redis_client.zcard("active_subscription_shards")
        print(f"   📺 Шардов подписок: {sub_shards_count}")
        
        if view_shards_count == 0 and sub_shards_count == 0:
            print("❌ НЕТ АКТИВНЫХ ШАРДОВ! Возможные причины:")
            print("   1. Не было создано постов в каналах")
            print("   2. Проблема с созданием шардов в task_service.py")
            print("   3. Шарды истекли и были удалены")
            return
        
        # ========== АНАЛИЗ ШАРДОВ ПРОСМОТРОВ ==========
        if view_shards_count > 0:
            print(f"\n🔍 АНАЛИЗ {view_shards_count} ШАРДОВ ПРОСМОТРОВ:")
            
            # Получаем все шарды просмотров
            all_view_shards = redis_client.zrangebyscore(
                "active_view_shards",
                min=0,
                max='+inf',
                withscores=True
            )
            
            print(f"   📋 Всего записей в active_view_shards: {len(all_view_shards)}")
            
            # Проверяем готовые шарды (как в воркере)
            ready_view_shards = redis_client.zrangebyscore(
                "active_view_shards",
                min=0,
                max=current_time + 1800,  # +30 минут
                withscores=False,
                start=0,
                num=10
            )
            
            print(f"   ⏰ Готовых к обработке (время <= {current_time + 1800}): {len(ready_view_shards)}")
            
            # Анализируем первые 3 шарда
            for i, (shard_meta_json, score) in enumerate(all_view_shards[:3]):
                print(f"\n   📦 ШАРД {i+1}:")
                try:
                    shard_meta = json.loads(shard_meta_json)
                    shard_key = shard_meta['shard_key']
                    shard_start_time = shard_meta.get('start_time', 0)
                    post_id = shard_meta.get('post_id', 'unknown')
                    
                    print(f"      🔑 Ключ: {shard_key}")
                    print(f"      📝 Пост ID: {post_id}")
                    print(f"      ⏰ Время начала: {shard_start_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(shard_start_time))})")
                    print(f"      📊 Score в ZSET: {score}")
                    
                    # Проверяем содержимое шарда
                    shard_size = redis_client.zcard(shard_key)
                    print(f"      📋 Задач в шарде: {shard_size}")
                    
                    if shard_size > 0:
                        # Получаем готовые задачи
                        ready_tasks = redis_client.zrangebyscore(
                            shard_key,
                            min=0,
                            max=current_time,
                            withscores=True,
                            start=0,
                            num=3
                        )
                        
                        print(f"      ✅ Готовых задач (время <= {current_time}): {len(ready_tasks)}")
                        
                        # Показываем пример задачи
                        if ready_tasks:
                            task_json, task_score = ready_tasks[0]
                            try:
                                task_data = json.loads(task_json)
                                print(f"      📄 Пример задачи:")
                                print(f"         📱 Телефон: {task_data.get('tasks', [{}])[0].get('phone', 'N/A')}")
                                print(f"         📺 Канал: {task_data.get('tasks', [{}])[0].get('channel', 'N/A')}")
                                print(f"         ⏰ Время выполнения: {task_score}")
                            except:
                                print(f"      ❌ Не удалось распарсить задачу")
                        
                        # Все задачи в шарде
                        all_tasks = redis_client.zrangebyscore(
                            shard_key,
                            min=0,
                            max='+inf',
                            withscores=True,
                            start=0,
                            num=5
                        )
                        
                        if all_tasks:
                            earliest_time = min(score for _, score in all_tasks)
                            latest_time = max(score for _, score in all_tasks)
                            print(f"      🕐 Диапазон времени задач:")
                            print(f"         Раннее: {earliest_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(earliest_time))})")
                            print(f"         Позднее: {latest_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(latest_time))})")
                            
                            # Проверяем почему задачи не готовы
                            if earliest_time > current_time:
                                wait_time = (earliest_time - current_time) / 60
                                print(f"      ⏳ ЗАДАЧИ ЕЩЕ НЕ ГОТОВЫ! Ждать: {wait_time:.1f} минут")
                            else:
                                print(f"      ✅ Задачи должны быть готовы!")
                    else:
                        print(f"      ❌ ШАРД ПУСТОЙ!")
                    
                except Exception as e:
                    print(f"      💥 Ошибка анализа шарда: {e}")
                    print(f"      📄 Raw data: {shard_meta_json[:100]}...")
            
            # Статистика по времени
            if all_view_shards:
                future_shards = [s for s in all_view_shards if s[1] > current_time + 1800]
                print(f"\n   📈 СТАТИСТИКА ВРЕМЕНИ:")
                print(f"      ⏰ Готовых сейчас: {len(ready_view_shards)}")
                print(f"      🔮 Будущих (>30 мин): {len(future_shards)}")
                
                if future_shards:
                    next_time = min(s[1] for s in future_shards)
                    wait_minutes = (next_time - current_time) / 60
                    print(f"      ⏳ Следующий шард через: {wait_minutes:.1f} минут")
        
        # ========== ПРОВЕРКА ДРУГИХ ОЧЕРЕДЕЙ ==========
        print(f"\n📋 ДРУГИЕ ОЧЕРЕДИ:")
        retry_count = redis_client.llen("retry_tasks")
        print(f"   🔄 Retry задач: {retry_count}")
        
        # Команды воркера
        worker_commands = redis_client.llen("worker_commands")
        print(f"   💬 Команд воркеру: {worker_commands}")
        
        # ========== РЕКОМЕНДАЦИИ ==========
        print(f"\n💡 РЕКОМЕНДАЦИИ:")
        
        if view_shards_count == 0:
            print("   1. ❌ Нет шардов - создайте пост в канале через бота")
            print("   2. 🔍 Проверьте создаются ли шарды в task_service.py")
        elif len(ready_view_shards) == 0:
            print("   1. ⏰ Все шарды в будущем - подождите или измените время")
            print("   2. 🔧 Проверьте настройку followPeriod.txt")
        else:
            print("   1. ✅ Шарды готовы - проблема в воркере")
            print("   2. 🔍 Проверьте логику _process_view_shards() в worker.py")
            print("   3. 🐛 Возможно проблема с сессиями аккаунтов")
        
        print(f"\n🔧 КОМАНДЫ ДЛЯ ОТЛАДКИ:")
        print(f"   redis-cli -h {REDIS_HOST} -p {REDIS_PORT} -a {REDIS_PASSWORD}")
        print(f"   ZCARD active_view_shards")
        print(f"   ZRANGE active_view_shards 0 2 WITHSCORES")
        
    except Exception as e:
        print(f"💥 ОШИБКА: {e}")
        print("🔧 Проверьте:")
        print("   1. Настройки Redis в config.py")
        print("   2. Доступность сервера Redis")
        print("   3. Правильность пароля")

if __name__ == "__main__":
    main()
