import asyncio
import asyncpg
from redis import Redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def clear_database():
    """Очистка базы данных"""
    try:
        conn = await asyncpg.connect(
            user='root',
            password='5f03f8eeb9:fx',
            database='arto_db',
            host='92.255.76.235',
            port='5432'
        )
        
        logger.info("🔍 Подключился к БД")
        
        # Получаем статистику до удаления
        accounts_count = await conn.fetchval("SELECT COUNT(*) FROM accounts")
        channels_count = await conn.fetchval("SELECT COUNT(*) FROM channels")
        languages_count = await conn.fetchval("SELECT COUNT(*) FROM languages")
        
        logger.info(f"📊 До удаления:")
        logger.info(f"   👥 Аккаунты: {accounts_count}")
        logger.info(f"   📺 Каналы: {channels_count}")
        logger.info(f"   🌐 Языки: {languages_count}")
        
        if accounts_count == 0 and channels_count == 0:
            logger.info("✅ БД уже пустая")
            return
        
        # Подтверждение
        confirm = input(f"\n⚠️ УДАЛИТЬ ВСЕ ДАННЫЕ? Введите 'YES' для подтверждения: ")
        if confirm != 'YES':
            logger.info("❌ Очистка отменена")
            return
        
        logger.info("🗑️ Очищаю БД...")
        
        # Удаляем в правильном порядке (внешние ключи)
        await conn.execute("DELETE FROM accounts")
        logger.info("✅ Аккаунты удалены")
        
        await conn.execute("DELETE FROM channels")
        logger.info("✅ Каналы удалены")
        
        await conn.execute("DELETE FROM statistics")
        logger.info("✅ Статистика удалена")
        
        # Языки можно оставить, они пригодятся
        # await conn.execute("DELETE FROM languages")
        
        # Сбрасываем счетчики AUTO_INCREMENT
        await conn.execute("ALTER SEQUENCE accounts_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE channels_id_seq RESTART WITH 1") 
        await conn.execute("ALTER SEQUENCE statistics_id_seq RESTART WITH 1")
        
        logger.info("✅ БД полностью очищена")
        await conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки БД: {e}")

def clear_redis():
    """Очистка Redis"""
    try:
        redis_client = Redis(
            host='92.255.76.235',
            port=6379,
            password='mybotredis2024',
            decode_responses=True
        )
        
        logger.info("🔍 Подключился к Redis")
        
        # Получаем статистику
        task_queues = [
            'delayed_view_batches',
            'subscription_tasks', 
            'retry_tasks'
        ]
        
        total_tasks = 0
        queue_stats = {}
        
        for queue in task_queues:
            count = redis_client.llen(queue)
            queue_stats[queue] = count
            total_tasks += count
        
        logger.info(f"📊 Задач в Redis:")
        for queue, count in queue_stats.items():
            logger.info(f"   📋 {queue}: {count}")
        logger.info(f"   📊 Всего: {total_tasks}")
        
        if total_tasks == 0:
            logger.info("✅ Redis уже пустой")
            return
        
        # Подтверждение
        confirm = input(f"\n⚠️ УДАЛИТЬ ВСЕ {total_tasks} ЗАДАЧ? Введите 'YES' для подтверждения: ")
        if confirm != 'YES':
            logger.info("❌ Очистка Redis отменена")
            return
        
        logger.info("🗑️ Очищаю Redis...")
        
        # Очищаем все очереди задач
        for queue in task_queues:
            deleted = redis_client.delete(queue)
            logger.info(f"✅ {queue}: удалено")
        
        # Дополнительно очищаем все ключи связанные с сессиями
        session_keys = redis_client.keys('session:*')
        if session_keys:
            redis_client.delete(*session_keys)
            logger.info(f"✅ Удалено {len(session_keys)} ключей сессий")
        
        logger.info("✅ Redis полностью очищен")
        redis_client.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки Redis: {e}")

async def main():
    print("🗑️ ПОЛНАЯ ОЧИСТКА СИСТЕМЫ")
    print("=" * 50)
    
    choice = input("""
Выберите что очистить:
1 - Только БД (аккаунты, каналы, статистика)
2 - Только Redis (все задачи)  
3 - ВСЁ (БД + Redis)
4 - Отмена

Ваш выбор (1-4): """)
    
    if choice == '1':
        await clear_database()
    elif choice == '2':
        clear_redis()
    elif choice == '3':
        await clear_database()
        clear_redis()
    elif choice == '4':
        logger.info("❌ Операция отменена")
    else:
        logger.error("❌ Неверный выбор")
    
    print("\n✅ Готово!")

if __name__ == "__main__":
    asyncio.run(main())
