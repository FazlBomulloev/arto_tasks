import asyncio
import logging
import json
import random
import time
from pathlib import Path
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, FSInputFile,
    InlineKeyboardButton as IKB, InlineKeyboardMarkup as IKM
)
from aiogram.utils.keyboard import InlineKeyboardBuilder as IKBuilder
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ChatType

# Добавляем утилиты для безопасной работы с сообщениями
async def safe_edit_message(message, text, parse_mode=None, reply_markup=None):
    """Безопасное редактирование сообщения"""
    try:
        await message.edit_text(
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
    except Exception as e:
        error_text = str(e).lower()
        if "message is not modified" in error_text or "not modified" in error_text:
            logger.debug("Сообщение не было изменено (содержимое идентично)")
        else:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            # Пытаемся отправить новое сообщение
            try:
                await message.answer(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            except:
                pass

from config import get_whitelist, find_english_word, find_russian_word, write_setting, read_setting
from database import (
    get_all_languages, add_language, get_channels_by_lang, add_channel,
    get_account_stats, get_accounts_by_lang, get_banned_accounts_24h
)
from account_service import account_service
from task_service import task_service
from exceptions import AccountValidationError, TaskProcessingError

logger = logging.getLogger(__name__)

# Декоратор для проверки whitelist
def whitelist_required(func):
    """Декоратор для проверки whitelist"""
    async def wrapper(call_or_message, *args, **kwargs):
        user_id = str(call_or_message.from_user.id)
        whitelist = get_whitelist()
        
        if user_id not in whitelist:
            if hasattr(call_or_message, 'answer'):  # CallbackQuery
                await call_or_message.answer("⛔ Доступ запрещен. Вы не в белом списке.", show_alert=True)
            else:  # Message
                await call_or_message.answer("⛔ Доступ запрещен. Вы не в белом списке.")
            return
        
        return await func(call_or_message, *args, **kwargs)
    return wrapper

# Состояния FSM
class BotStates(StatesGroup):
    # Языки
    waiting_channel_name = State()
    waiting_accounts_count = State()
    waiting_zip_file = State() 
    
    # Настройки
    waiting_setting_value = State()
    
    # Аккаунты
    waiting_delete_count = State()

# Роутеры
main_router = Router()
lang_router = Router()
account_router = Router()
settings_router = Router()
stats_router = Router()

# === ГЛАВНОЕ МЕНЮ ===

@main_router.message(Command('start'))
async def start_command(message: Message, state: FSMContext):
    """Стартовое меню с проверкой whitelist"""
    await state.clear()
    
    user_id = str(message.from_user.id)
    whitelist = get_whitelist()
    
    if user_id not in whitelist:
        await message.answer("⛔ Доступ запрещен. Вы не в белом списке.")
        return
    
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🌐 ЯЗЫКИ', callback_data='languages')],
        [IKB(text='👥 АККАУНТЫ', callback_data='accounts')],
        [IKB(text='⚙️ НАСТРОЙКИ', callback_data='settings')],
        [IKB(text='📊 СТАТИСТИКА', callback_data='statistics')]
    ])
    
    await message.answer(
        "<b>🤖 ГЛАВНОЕ МЕНЮ</b>\n\n"
        "🌐 Управление языками и каналами\n"
        "👥 Управление аккаунтами\n"
        "⚙️ Настройки задержек\n"
        "📊 Статистика работы\n\n",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@main_router.callback_query(F.data == 'main_menu')
async def back_to_main(call: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    
    # Проверяем whitelist
    user_id = str(call.from_user.id)
    whitelist = get_whitelist()
    
    if user_id not in whitelist:
        await call.answer("⛔ Доступ запрещен. Вы не в белом списке.", show_alert=True)
        return
    
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🌐 ЯЗЫКИ', callback_data='languages')],
        [IKB(text='👥 АККАУНТЫ', callback_data='accounts')],
        [IKB(text='⚙️ НАСТРОЙКИ', callback_data='settings')],
        [IKB(text='📊 СТАТИСТИКА', callback_data='statistics')]
    ])
    
    await call.message.edit_text(
        "<b>🤖 ГЛАВНОЕ МЕНЮ</b>\n\n"
        "🌐 Управление языками и каналами\n"
        "👥 Управление аккаунтами\n"
        "⚙️ Настройки задержек\n"
        "📊 Статистика работы\n\n"
        ,
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await call.answer()

# === ЯЗЫКИ И КАНАЛЫ ===

@whitelist_required
@lang_router.callback_query(F.data == 'languages')
async def languages_menu(call: CallbackQuery):
    """Меню управления языками"""
    try:
        languages = await get_all_languages()
        
        keyboard = IKBuilder()
        
        # Кнопки языков
        for lang in languages:
            keyboard.add(IKB(text=lang, callback_data=f'lang:{lang}'))
        keyboard.adjust(2)
        
        # Управляющие кнопки
        keyboard.row(IKB(text='➕ ДОБАВИТЬ ЯЗЫК', callback_data='add_language'))
        keyboard.row(IKB(text='🔙 НАЗАД', callback_data='main_menu'))
        
        # Формируем текст с статистикой
        text_parts = ["<b>🌐 УПРАВЛЕНИЕ ЯЗЫКАМИ</b>\n"]
        
        for lang in languages:
            english_lang = find_english_word(lang)
            accounts = await get_accounts_by_lang(english_lang, 'active')
            channels = await get_channels_by_lang(lang)
            
            text_parts.append(
                f"<b>{lang}</b> | Аккаунтов: {len(accounts)} | Каналов: {len(channels)}"
            )
        
        if not languages:
            text_parts.append("<i>Языки не добавлены</i>")
        
        await safe_edit_message(
            call.message,
            "\n".join(text_parts),
            parse_mode='HTML',
            reply_markup=keyboard.as_markup()
        )
        
    except Exception as e:
        logger.error(f"Ошибка меню языков: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@lang_router.callback_query(F.data.startswith('lang:'))
async def language_details(call: CallbackQuery):
    """Детали конкретного языка"""
    try:
        lang = call.data.split(':', 1)[1]
        english_lang = find_english_word(lang)
        
        # Получаем данные
        accounts = await get_accounts_by_lang(english_lang)
        channels = await get_channels_by_lang(lang)
        
        # Статистика по статусам
        active_count = len([a for a in accounts if a['status'] == 'active'])
        pause_count = len([a for a in accounts if a['status']== 'pause'])
        ban_count = len([a for a in accounts if a['status'] == 'ban'])
        
        # Список каналов с кнопками удаления
        channels_keyboard = IKBuilder()
        if channels:
            for ch in channels:
                channels_keyboard.row(
                    IKB(text=f"@{ch}", url=f"https://t.me/{ch}"),
                    IKB(text="🗑️", callback_data=f'delete_channel:{lang}:{ch}')
                )
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='➕ ДОБАВИТЬ КАНАЛ', callback_data=f'add_channel:{lang}')],
            [IKB(text='➕ ДОБАВИТЬ АККАУНТЫ', callback_data=f'add_accounts:{lang}')],
            [IKB(text='📤 ЭКСПОРТ АКТИВНЫХ', callback_data=f'export_accounts:{lang}')],
            [IKB(text='🗑️ УДАЛИТЬ АККАУНТЫ', callback_data=f'manage_accounts:{lang}')],
            [IKB(text='🔙 НАЗАД', callback_data='languages')]
        ])
        
        # Объединяем клавиатуры
        if channels:
            keyboard.inline_keyboard = channels_keyboard.as_markup().inline_keyboard + keyboard.inline_keyboard
        
        # Список каналов
        channels_text = ""
        if channels:
            channels_text = f"\n<b>📺 Каналы ({len(channels)}):</b>\n"
            channels_text += "\n".join([f"• @{ch}" for ch in channels])
        else:
            channels_text = "\n<b>📺 Каналы:</b>\n<i>Каналы не добавлены</i>"
        
        text = f"""<b>🌐 ЯЗЫК: {lang.upper()}</b>

<b>📊 Статистика аккаунтов:</b>
✅ Активные: {active_count}
⏸️ На паузе: {pause_count}  
🚫 Забанены: {ban_count}
📱 Всего: {len(accounts)}

{channels_text} """
        
        await safe_edit_message(
            call.message,
            text,
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка деталей языка: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@lang_router.callback_query(F.data.startswith('add_channel:'))
async def add_channel_start(call: CallbackQuery, state: FSMContext):
    """Начало добавления канала"""
    lang = call.data.split(':', 1)[1]
    
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🔙 НАЗАД', callback_data=f'lang:{lang}')]
    ])
    
    await call.message.edit_text(
        f"<b>➕ ДОБАВЛЕНИЕ КАНАЛА</b>\n\n"
        f"Язык: <b>{lang}</b>\n\n"
        f"📝 Отправьте имя канала без @ (например: <code>mychannel</code>)\n\n"
        f"⚡ После добавления будут созданы задачи подписки для всех аккаунтов языка",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
    await state.set_state(BotStates.waiting_channel_name)
    await state.update_data(lang=lang, message_id=call.message.message_id)

@lang_router.message(BotStates.waiting_channel_name)
async def add_channel_process(message: Message, state: FSMContext):
    """Обработка добавления канала"""
    try:
        data = await state.get_data()
        lang = data['lang']
        channel_name = message.text.strip().replace('@', '')
        
        # Добавляем канал в БД
        success = await add_channel(channel_name, lang)
        
        await message.delete()
        
        if success:
            # Создаем задачи подписки
            try:
                results = await task_service.create_subscription_tasks(channel_name, lang)
                
                keyboard = IKM(inline_keyboard=[
                    [IKB(text='🔙 К ЯЗЫКУ', callback_data=f'lang:{lang}')]
                ])
                
                await message.answer(
                    f"✅ <b>Канал @{channel_name} добавлен!</b>\n\n"
                    f"📊 Создано задач подписки: {results['total_tasks']}\n"
                    f"👥 Аккаунтов задействовано: {results['accounts_processed']}\n\n",
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            except Exception as e:
                keyboard = IKM(inline_keyboard=[
                    [IKB(text='🔙 К ЯЗЫКУ', callback_data=f'lang:{lang}')]
                ])
                await message.answer(
                    f"✅ Канал @{channel_name} добавлен в БД\n"
                    f"❌ Ошибка создания задач подписки: {str(e)[:200]}",
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
        else:
            await message.answer(f"❌ Не удалось добавить канал @{channel_name}")
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        await message.answer("❌ Произошла ошибка при добавлении канала")
        await state.clear()

@lang_router.callback_query(F.data.startswith('delete_channel:'))
async def delete_channel_confirm(call: CallbackQuery):
    """Подтверждение удаления канала"""
    try:
        _, lang, channel_name = call.data.split(':', 2)
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='✅ ДА, УДАЛИТЬ', callback_data=f'confirm_delete_channel:{lang}:{channel_name}')],
            [IKB(text='❌ ОТМЕНА', callback_data=f'lang:{lang}')]
        ])
        
        await safe_edit_message(
            call.message,
            f"⚠️ <b>УДАЛЕНИЕ КАНАЛА</b>\n\n"
            f"Вы точно хотите удалить канал <b>@{channel_name}</b> из языка <b>{lang}</b>?\n\n"
            f"🚨 Это действие <b>НЕОБРАТИМО</b>!",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка подтверждения удаления канала: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@lang_router.callback_query(F.data.startswith('confirm_delete_channel:'))
async def delete_channel_execute(call: CallbackQuery):
    """Выполнение удаления канала"""
    try:
        _, lang, channel_name = call.data.split(':', 2)
        
        # Удаляем канал из БД
        from database import delete_channel
        success = await delete_channel(channel_name, lang)
        
        if success:
            await call.answer(f"✅ Канал @{channel_name} удален!", show_alert=True)
            logger.info(f"🗑️ Удален канал @{channel_name} из языка {lang}")
        else:
            await call.answer(f"❌ Канал @{channel_name} не найден", show_alert=True)
        
        # Возвращаемся к деталям языка
        await language_details(call)
        
    except Exception as e:
        logger.error(f"Ошибка удаления канала: {e}")
        await call.answer("❌ Произошла ошибка при удалении", show_alert=True)

@account_router.callback_query(F.data.startswith('export_accounts:'))
async def export_accounts_by_lang(call: CallbackQuery):
    """Экспорт активных аккаунтов по языку"""
    lang = call.data.split(':', 1)[1]
    
    try:
        progress_msg = await call.message.edit_text(
            "📦 <b>Создание архива...</b>\n⏳ Собираю активные аккаунты...",
            parse_mode='HTML'
        )
        
        # Создаем архив
        archive_path = await account_service.export_active_accounts(lang)
        
        if archive_path and archive_path.exists():
            # Отправляем файл
            await call.message.answer_document(
                FSInputFile(archive_path, filename=archive_path.name),
                caption=f"📦 <b>Архив активных аккаунтов</b>\n🌐 Язык: {lang}",
                parse_mode='HTML'
            )
            
            # Удаляем временный файл
            archive_path.unlink()
        else:
            await progress_msg.edit_text(
                "❌ <b>Не удалось создать архив</b>\n\n"
                "Возможно, нет активных аккаунтов для экспорта",
                parse_mode='HTML'
            )
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 К ЯЗЫКУ', callback_data=f'lang:{lang}')]
        ])
        await progress_msg.edit_reply_markup(reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка экспорта аккаунтов: {e}")
        await call.answer("❌ Произошла ошибка при создании архива", show_alert=True)

@account_router.callback_query(F.data == 'export_all_active')
async def export_all_accounts(call: CallbackQuery):
    """Экспорт всех активных аккаунтов"""
    try:
        progress_msg = await call.message.edit_text(
            "📦 <b>Создание полного архива...</b>\n⏳ Собираю все активные аккаунты...",
            parse_mode='HTML'
        )
        
        # Создаем архив всех активных
        archive_path = await account_service.export_active_accounts()
        
        if archive_path and archive_path.exists():
            # Отправляем файл
            await call.message.answer_document(
                FSInputFile(archive_path, filename=archive_path.name),
                caption="📦 <b>Архив всех активных аккаунтов</b>",
                parse_mode='HTML'
            )
            
            # Удаляем временный файл
            archive_path.unlink()
        else:
            await progress_msg.edit_text(
                "❌ <b>Не удалось создать архив</b>\n\n"
                "Возможно, нет активных аккаунтов для экспорта",
                parse_mode='HTML'
            )
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 НАЗАД', callback_data='accounts')]
        ])
        await progress_msg.edit_reply_markup(reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка экспорта всех аккаунтов: {e}")
        await call.answer("❌ Произошла ошибка при создании архива", show_alert=True)

# === УПРАВЛЕНИЕ АККАУНТАМИ ===
@account_router.callback_query(F.data == 'accounts')
async def accounts_menu(call: CallbackQuery):
    """Главное меню аккаунтов"""
    try:
        stats = await get_account_stats()
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🗑️ УДАЛИТЬ ПО СТАТУСУ', callback_data='delete_by_status')],
            [IKB(text='📤 ЭКСПОРТ АКТИВНЫХ', callback_data='export_all_active')],
            [IKB(text='🔙 НАЗАД', callback_data='main_menu')]
        ])
        
        text = f"""<b>👥 УПРАВЛЕНИЕ АККАУНТАМИ</b>

<b>📊 Общая статистика:</b>
📱 Всего аккаунтов: {stats.get('total', 0)}
✅ Активных: {stats.get('active', 0)}
⏸️ На паузе: {stats.get('pause', 0)}
🚫 Забанены: {stats.get('ban', 0)}
"""
        
        await safe_edit_message(
            call.message,
            text,
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка меню аккаунтов: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@account_router.callback_query(F.data.startswith('add_accounts:'))
async def add_accounts_start(call: CallbackQuery, state: FSMContext):
    """Начало добавления аккаунтов"""
    lang = call.data.split(':', 1)[1]
    
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🔙 НАЗАД', callback_data=f'lang:{lang}')]
    ])
    
    await call.message.edit_text(
        f"<b>➕ ДОБАВЛЕНИЕ АККАУНТОВ</b>\n\n"
        f"Язык: <b>{lang}</b>\n\n"
        f"📦 Отправьте ZIP архив с аккаунтами\n\n"
        f"• Выберете режим проверки\n"
        f"• Быстрое добавление или с валидацией\n",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
    await state.set_state(BotStates.waiting_zip_file)
    await state.update_data(lang=lang, message_id=call.message.message_id)

@account_router.message(BotStates.waiting_zip_file)
async def add_accounts_process(message: Message, state: FSMContext):
    """Обработка ZIP файла с аккаунтами"""
    try:
        if not message.document or not message.document.file_name.lower().endswith('.zip'):
            await message.answer("❌ Отправьте ZIP файл")
            return
        
        data = await state.get_data()
        lang = data['lang']
        
        # Скачиваем файл
        file_info = await message.bot.get_file(message.document.file_id)
        zip_path = Path(f"downloads/upload_{int(asyncio.get_event_loop().time())}.zip")
        zip_path.parent.mkdir(exist_ok=True)
        
        await message.bot.download_file(file_info.file_path, zip_path)
        await message.delete()
        
        # СПРАШИВАЕМ О РЕЖИМЕ ПРОВЕРКИ
        keyboard = IKM(inline_keyboard=[
            [IKB(text='✅ ДА, ПРОВЕРИТЬ', callback_data=f'validate_accounts:{lang}:true')],
            [IKB(text='⚡ НЕТ, БЫСТРО ДОБАВИТЬ', callback_data=f'validate_accounts:{lang}:false')],
            [IKB(text='❌ ОТМЕНА', callback_data=f'lang:{lang}')]
        ])
        
        await message.answer(
            f"<b>🔍 РЕЖИМ ОБРАБОТКИ АККАУНТОВ</b>\n\n"
            f"Язык: <b>{lang}</b>\n"
            f"Архив: <b>{message.document.file_name}</b>\n\n"
            f"<b>Выберите режим:</b>\n\n"
            f"✅ <b>С ПРОВЕРКОЙ</b> - медленно, но надежно:\n"
            f"   • Подключение к каждому аккаунту\n"
            f"   • Проверка авторизации\n"
            f"   • Только рабочие аккаунты в БД\n\n"
            f"⚡ <b>БЫСТРОЕ ДОБАВЛЕНИЕ</b> - мгновенно:\n"
            f"   • Конвертация без подключения\n"
            f"   • Проверка при выполнении задач\n"
            f"   • Все аккаунты сразу в БД",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
        # Сохраняем путь к файлу в состоянии
        await state.update_data(zip_path=str(zip_path))
        
    except Exception as e:
        logger.error(f"Ошибка обработки ZIP: {e}")
        await message.answer("❌ Произошла ошибка при обработке файла")
        await state.clear()

@account_router.callback_query(F.data.startswith('validate_accounts:'))
async def process_accounts_with_choice(call: CallbackQuery, state: FSMContext):
    """Обработка аккаунтов в выбранном режиме"""
    try:
        _, lang, validate_str = call.data.split(':', 2)
        validate_accounts = validate_str.lower() == 'true'
        
        data = await state.get_data()
        zip_path = Path(data['zip_path'])
        
        if not zip_path.exists():
            await call.answer("❌ Файл не найден", show_alert=True)
            return
        
        mode_text = "С ПРОВЕРКОЙ" if validate_accounts else "БЫСТРОЕ ДОБАВЛЕНИЕ"
        
        progress_msg = await call.message.edit_text(
            f"🔄 <b>{mode_text}</b>\n"
            f"📦 Извлекаю архив...",
            parse_mode='HTML'
        )
        
        async def update_progress(text):
            try:
                await progress_msg.edit_text(f"🔄 <b>{mode_text}</b>\n{text}", parse_mode='HTML')
            except:
                pass
        
        # Используем новый единый метод с параметром валидации
        results = await account_service.add_accounts_from_zip(
            zip_path, lang, validate_accounts, update_progress
        )
        
        # Удаляем временный файл
        if zip_path.exists():
            zip_path.unlink()
        
        # Показываем результаты
        success_rate = (results['added'] / results['total']) * 100 if results['total'] > 0 else 0
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 К ЯЗЫКУ', callback_data=f'lang:{lang}')]
        ])
        
        validation_text = "Все аккаунты проверены" if validate_accounts else "Проверка при выполнении задач"
        
        await progress_msg.edit_text(
            f"✅ <b>{mode_text} завершено!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"📱 Всего аккаунтов: {results['total']}\n"
            f"➕ Добавлено: {results['added']}\n"
            f"⏭️ Уже было: {results.get('skipped_exists', 0)}\n"
            f"❌ Не удалось: {results.get('failed_validation', 0) + results.get('failed_db', 0)}\n"
            f"📈 Успешность: {success_rate:.1f}%\n\n"
            f"⚡ <b>Статус:</b> {validation_text}\n"
            f"📺 Созданы задачи подписки на все каналы языка",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка обработки аккаунтов: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)
        await state.clear()

@account_router.callback_query(F.data == 'delete_by_status')
async def delete_by_status_menu(call: CallbackQuery):
    """Меню удаления по статусу"""
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🚫 УДАЛИТЬ ЗАБАНЕННЫХ', callback_data='delete_status:ban')],
        [IKB(text='⏸️ УДАЛИТЬ НА ПАУЗЕ', callback_data='delete_status:pause')],
        [IKB(text='🗑️ УДАЛИТЬ ВСЕ', callback_data='delete_status:all')],
        [IKB(text='🔙 НАЗАД', callback_data='accounts')]
    ])
    
    await call.message.edit_text(
        "<b>🗑️ УДАЛЕНИЕ АККАУНТОВ ПО СТАТУСУ</b>\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b> Удаление необратимо!\n"
        "Аккаунты будут полностью удалены из базы данных.\n\n",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@account_router.callback_query(F.data.startswith('delete_status:'))
async def delete_by_status_confirm(call: CallbackQuery):
    """Подтверждение удаления по статусу"""
    status = call.data.split(':', 1)[1]
    
    # Получаем количество для удаления
    try:
        if status == 'all':
            stats = await get_account_stats()
            count = stats.get('total', 0)
            status_text = "ВСЕХ"
        else:
            stats = await get_account_stats()
            count = stats.get(status, 0)
            status_text = {
                'ban': 'ЗАБАНЕННЫХ',
                'pause': 'НА ПАУЗЕ'
            }.get(status, status.upper())
        
        if count == 0:
            await call.answer(f"❌ Нет аккаунтов со статусом '{status}'", show_alert=True)
            return
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='✅ ДА, УДАЛИТЬ', callback_data=f'confirm_delete:{status}')],
            [IKB(text='❌ ОТМЕНА', callback_data='delete_by_status')]
        ])
        
        await call.message.edit_text(
            f"⚠️ <b>ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ</b>\n\n"
            f"Вы точно хотите удалить <b>{count}</b> {status_text} аккаунтов?\n\n"
            f"🚨 Это действие <b>НЕОБРАТИМО</b>!\n\n",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка подтверждения удаления: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@account_router.callback_query(F.data.startswith('confirm_delete:'))
async def delete_by_status_execute(call: CallbackQuery):
    """Выполнение удаления по статусу"""
    status = call.data.split(':', 1)[1]
    
    try:
        progress_msg = await call.message.edit_text(
            "🔄 <b>Удаление аккаунтов...</b>\n⏳ Пожалуйста, подождите...",
            parse_mode='HTML'
        )
        
        # Выполняем удаление
        deleted_count = await account_service.delete_accounts_by_status(status)
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 НАЗАД', callback_data='accounts')]
        ])
        
        if deleted_count > 0:
            await progress_msg.edit_text(
                f"✅ <b>Удаление завершено</b>\n\n"
                f"🗑️ Удалено аккаунтов: {deleted_count}\n\n",
                parse_mode='HTML',
                reply_markup=keyboard
            )
        else:
            await progress_msg.edit_text(
                "❌ <b>Ничего не удалено</b>\n\n"
                "Возможно, аккаунты с таким статусом не найдены",
                parse_mode='HTML',
                reply_markup=keyboard
            )
        
    except Exception as e:
        logger.error(f"Ошибка удаления аккаунтов: {e}")
        await call.answer("❌ Произошла ошибка при удалении", show_alert=True)

# === НАСТРОЙКИ СМЕШАННЫХ БАТЧЕЙ ===
@settings_router.callback_query(F.data == 'settings')
async def settings_menu(call: CallbackQuery):
    """Меню настроек со смешанными батчами"""
    try:
        # Читаем настройки включая новые параметры смешанных батчей
        settings = {
            'view_period': read_setting('followPeriod.txt', 1.0),
            
            # НОВЫЕ настройки просмотров
            'view_reading_time': read_setting('view_reading_time.txt', 5.0),
            'view_connection_pause': read_setting('view_connection_pause.txt', 3.0),
            
            # НОВЫЕ настройки смешанных батчей
            'mixed_batch_size': int(read_setting('mixed_batch_size.txt', 500.0)),
            'mixed_batch_pause': read_setting('mixed_batch_pause.txt', 30.0),
            
            # Настройки подписок
            'sub_lag': read_setting('lag.txt', 30.0),
            'sub_range': read_setting('range.txt', 5.0),
            'timeout_count': int(read_setting('timeout_count.txt', 4.0)),
            'timeout_duration': read_setting('timeout_duration.txt', 20.0),
            'accounts_delay': read_setting('accounts_delay.txt', 2.0)
        }
        
        keyboard = IKM(inline_keyboard=[
            # НОВЫЕ настройки смешанных батчей
            [IKB(text='📦 РАЗМЕР СМЕШАННОГО БАТЧА', callback_data='set:mixed_batch_size.txt')],
            [IKB(text='⏸️ ПАУЗА МЕЖДУ БАТЧАМИ', callback_data='set:mixed_batch_pause.txt')],
            
            # Разделитель
            [IKB(text='── ПАРАМЕТРЫ ПРОСМОТРОВ ──', callback_data='separator')],
            [IKB(text='📖 ВРЕМЯ ПРОСМОТРА ПУБЛИКАЦИИ', callback_data='set:view_reading_time.txt')],
            [IKB(text='🔌 ПАУЗА ПОДКЛЮЧ/ВЫКЛЮЧЕНИЕ', callback_data='set:view_connection_pause.txt')],
            [IKB(text='⏰ ПЕРИОД ПРОСМОТРОВ', callback_data='set:followPeriod.txt')],
            
            # Разделитель
            [IKB(text='── НАСТРОЙКИ ПОДПИСОК ──', callback_data='separator')],
            [IKB(text='📅 ОСНОВНАЯ ЗАДЕРЖКА', callback_data='set:lag.txt')],
            [IKB(text='🎲 РАЗБРОС ПОДПИСКИ', callback_data='set:range.txt')],
            [IKB(text='⏰ ЗАДЕРЖКА АККАУНТОВ', callback_data='set:accounts_delay.txt')],
            [IKB(text='🔢 ПОДПИСОК ДО ПАУЗЫ', callback_data='set:timeout_count.txt')],
            [IKB(text='⏸️ ДЛИТЕЛЬНОСТЬ ПАУЗЫ', callback_data='set:timeout_duration.txt')],
            
            [IKB(text='🔄 ОБНОВИТЬ ВСЕ', callback_data='force_settings_reload')],
            [IKB(text='🔙 НАЗАД', callback_data='main_menu')]
        ])
        
        text = f"""<b>⚙️ НАСТРОЙКИ СИСТЕМЫ</b>

<b>📦 СМЕШАННЫЕ БАТЧИ:</b>
📊 Размер батча: {settings['mixed_batch_size']} задач (любых типов)
⏸️ Пауза между батчами: {settings['mixed_batch_pause']} сек

<b>👀 ПАРАМЕТРЫ ПРОСМОТРОВ:</b>
📖 Время просмотра публикации: {settings['view_reading_time']} сек
🔌 Пауза подключ/выключение: {settings['view_connection_pause']} сек
⏰ Период просмотров: {settings['view_period']} час

<b>📺 НАСТРОЙКИ ПОДПИСОК:</b>
📅 Основная задержка: {settings['sub_lag']} мин
🎲 Разброс: {settings['sub_range']} мин
⏰ Задержка аккаунтов: {settings['accounts_delay']} мин
🔢 Подписок до паузы: {settings['timeout_count']}
⏸️ Длительность паузы: {settings['timeout_duration']} мин

<b>📋 ЛОГИКА СМЕШАННОГО БАТЧА:</b>
• В одном батче: просмотры + подписки (любое соотношение)
• Все {settings['mixed_batch_size']} задач выполняются параллельно
• Просмотры: Подключился → Пауза {settings['view_connection_pause']}с → Просмотр {settings['view_reading_time']}с → Пауза {settings['view_connection_pause']}с → Отключился
• Между батчами пауза {settings['mixed_batch_pause']}с"""
        
        await call.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка меню настроек: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@settings_router.callback_query(F.data.startswith('set:'))
async def setting_change_start(call: CallbackQuery, state: FSMContext):
    """Начало изменения настройки с новыми параметрами смешанных батчей"""
    setting_file = call.data.split(':', 1)[1]
    
    setting_names = {
        # НОВЫЕ параметры смешанных батчей
        'mixed_batch_size.txt': 'Размер смешанного батча (количество задач любых типов)',
        'mixed_batch_pause.txt': 'Пауза между смешанными батчами (секунды)',
        
        # Параметры просмотров
        'view_reading_time.txt': 'Время просмотра публикации (секунды)',
        'view_connection_pause.txt': 'Пауза подключ/выключение (секунды)',
        'followPeriod.txt': 'Период просмотров (часы)',
        
        # Настройки подписок
        'lag.txt': 'Основная задержка подписок (минуты)',
        'range.txt': 'Разброс подписок (минуты)',
        'accounts_delay.txt': 'Задержка аккаунтов (минуты)',
        'timeout_count.txt': 'Количество подписок до паузы',
        'timeout_duration.txt': 'Длительность паузы (минуты)'
    }
    
    setting_name = setting_names.get(setting_file, setting_file)
    current_value = read_setting(setting_file, 0)
    
    # Подсказки для новых параметров
    hints = {
        'mixed_batch_size.txt': '\n💡 Рекомендуется: 300-1000\nОбщее количество задач в батче (просмотры + подписки)',
        'mixed_batch_pause.txt': '\n💡 Рекомендуется: 20-60 секунд\nВремя отдыха между батчами для снижения нагрузки',
        'view_reading_time.txt': '\n💡 Рекомендуется: 3-8 секунд\nВремя "чтения" поста каждым аккаунтом',
        'view_connection_pause.txt': '\n💡 Рекомендуется: 2-5 секунд\nПауза до и после просмотра для имитации естественного поведения'
    }
    
    hint_text = hints.get(setting_file, '')
    
    keyboard = IKM(inline_keyboard=[
        [IKB(text='🔙 НАЗАД', callback_data='settings')]
    ])
    
    await call.message.edit_text(
        f"<b>⚙️ ИЗМЕНЕНИЕ НАСТРОЙКИ</b>\n\n"
        f"📝 Параметр: <b>{setting_name}</b>\n"
        f"🔢 Текущее значение: <b>{current_value}</b>\n\n"
        f"✏️ Введите новое значение:{hint_text}\n\n",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
    await state.set_state(BotStates.waiting_setting_value)
    await state.update_data(setting_file=setting_file, setting_name=setting_name)

@settings_router.message(BotStates.waiting_setting_value)
async def setting_change_process(message: Message, state: FSMContext):
    """Обработка изменения настройки"""
    try:
        data = await state.get_data()
        setting_file = data['setting_file']
        setting_name = data['setting_name']
        
        # Проверяем что введено число
        try:
            new_value = float(message.text.strip())
            if new_value < 0:
                await message.answer("❌ Значение должно быть положительным числом")
                return
        except ValueError:
            await message.answer("❌ Введите корректное число")
            return
        
        # Сохраняем настройку
        write_setting(setting_file, str(new_value))
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 К НАСТРОЙКАМ', callback_data='settings')]
        ])
        
        await message.delete()
        await message.answer(
            f"✅ <b>Настройка обновлена</b>\n\n"
            f"📝 {setting_name}: <b>{new_value}</b>\n\n",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка изменения настройки: {e}")
        await message.answer("❌ Произошла ошибка при сохранении")
        await state.clear()

@settings_router.callback_query(F.data == 'force_settings_reload')
async def force_settings_reload(call: CallbackQuery):
    """Принудительное обновление настроек в воркере"""
    try:
        progress_msg = await call.message.edit_text(
            "🔄 <b>Обновление настроек...</b>\n⏳ Отправляю сигнал воркеру...",
            parse_mode='HTML'
        )
        
        # Отправляем сигнал воркеру через Redis
        from redis import Redis
        from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
        
        redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        
        # Отправляем команду воркеру
        redis_client.lpush('worker_commands', json.dumps({
            'command': 'reload_settings',
            'timestamp': time.time()
        }))
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 К НАСТРОЙКАМ', callback_data='settings')]
        ])
        
        await progress_msg.edit_text(
            "✅ <b>Настройки обновлены!</b>\n\n"
            "🔄 Воркер получил сигнал обновления\n"
            "📊 Новые настройки применятся к следующим задачам\n\n",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка обновления настроек: {e}")
        await call.answer("❌ Ошибка обновления", show_alert=True)

@settings_router.callback_query(F.data == 'separator')  
async def separator_handler(call: CallbackQuery):
    """Обработчик разделителей (ничего не делает)"""
    await call.answer()

# === УПРОЩЕННАЯ СТАТИСТИКА ===

async def get_simplified_statistics():
    """Получает упрощенную статистику согласно требованиям"""
    try:
        from redis import Redis
        from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
        
        redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        
        current_time = time.time()
        
        # Базовая статистика задач из Redis
        total_tasks = redis_client.zcard("task_queue") or 0
        
        # Получаем статистику выполненных задач из воркера
        worker_stats_raw = redis_client.get('worker_stats')
        if worker_stats_raw:
            worker_stats = json.loads(worker_stats_raw)
            
            # Проверяем актуальность данных (не старше 5 минут)
            stats_age = current_time - worker_stats.get('timestamp', 0)
            if stats_age <= 300:  # 5 минут
                tasks_last_60min = worker_stats.get('tasks_last_hour', 0)
                tasks_last_24h = worker_stats.get('tasks_last_24h', 0)
            else:
                tasks_last_60min = 0
                tasks_last_24h = 0
        else:
            tasks_last_60min = 0
            tasks_last_24h = 0
        
        # Рассчитываем среднее количество задач за секунду (за последние 60 минут)
        avg_tasks_per_sec = tasks_last_60min / 3600 if tasks_last_60min > 0 else 0
        
        # Рассчитываем ориентировочное время выполнения просмотров (в часах)
        # Формула: все задачи за 24ч / задачи за 60мин = время в часах
        if tasks_last_60min > 0:
            estimated_hours = tasks_last_24h / tasks_last_60min
        else:
            estimated_hours = 0
        
        # Статистика аккаунтов
        account_stats = await get_account_stats()
        
        # Забаненные за 24ч
        banned_24h = await get_banned_accounts_24h()
        
        return {
            # Аккаунты
            'total_accounts': account_stats.get('total', 0),
            'active_accounts': account_stats.get('active', 0),
            'paused_accounts': account_stats.get('pause', 0),
            'banned_accounts': account_stats.get('ban', 0),
            'banned_24h': banned_24h,
            
            # Задачи
            'total_tasks_redis': total_tasks,
            'avg_tasks_per_second': avg_tasks_per_sec,
            'estimated_completion_hours': estimated_hours,
            
            # Время обновления
            'updated_at': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time)),
            'stats_age': stats_age if worker_stats_raw else 999999
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения упрощенной статистики: {e}")
        return {}

@stats_router.callback_query(F.data == 'statistics')
async def statistics_menu(call: CallbackQuery):
    """Главное меню статистики (упрощенное согласно требованиям)"""
    try:
        stats = await get_simplified_statistics()
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='📊 ПО ЯЗЫКАМ', callback_data='stats_by_lang')],
            [IKB(text='🔄 ОБНОВИТЬ', callback_data='statistics')],
            [IKB(text='🔙 НАЗАД', callback_data='main_menu')]
        ])
        
        # Форматируем время выполнения
        est_hours = stats.get('estimated_completion_hours', 0)
        if est_hours > 24:
            time_str = f"{est_hours/24:.1f} дней"
        elif est_hours > 1:
            time_str = f"{est_hours:.1f} часов"
        else:
            time_str = f"{est_hours*60:.0f} минут"
        
        text = f"""<b>📊 СТАТИСТИКА</b>

<b>👥 АККАУНТЫ:</b>
📱 Всего: {stats.get('total_accounts', 0)}
✅ Активные: {stats.get('active_accounts', 0)}
⏸️ На паузе: {stats.get('paused_accounts', 0)}
🚫 Забанены: {stats.get('banned_accounts', 0)}
🚫 Забанены за 24ч: {stats.get('banned_24h', 0)}

<b>📋 ЗАДАЧИ:</b>
📦 Всего в Redis: {stats.get('total_tasks_redis', 0)}
⚡ Среднее задач/сек: {stats.get('avg_tasks_per_second', 0):.2f}
⏱️ Ориент. время выполнения: {time_str}

<b>🕐 Обновлено:</b> {stats.get('updated_at', 'Неизвестно')}"""
        
        await call.message.edit_text(
            text,
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@stats_router.callback_query(F.data == 'stats_by_lang')
async def stats_by_language(call: CallbackQuery):
    """Статистика по языкам"""
    try:
        account_stats = await get_account_stats()
        languages = await get_all_languages()
        
        keyboard = IKM(inline_keyboard=[
            [IKB(text='🔙 НАЗАД', callback_data='statistics')]
        ])
        
        text_parts = ["<b>📊 СТАТИСТИКА ПО ЯЗЫКАМ</b>\n"]
        
        by_language = account_stats.get('by_language', {})
        
        for lang in languages:
            english_lang = find_english_word(lang)
            total = by_language.get(english_lang, 0)
            
            # Получаем детальную статистику
            accounts = await get_accounts_by_lang(english_lang)
            active = len([a for a in accounts if a['status'] == 'active'])
            pause = len([a for a in accounts if a['status'] == 'pause'])
            ban = len([a for a in accounts if a['status'] == 'ban'])
            
            text_parts.append(
                f"<b>{lang}:</b>\n"
                f"  📱 Всего: {total}\n"
                f"  ✅ Активных: {active}\n"
                f"  ⏸️ На паузе: {pause}\n"
                f"  🚫 Забанены: {ban}\n"
            )
        
        if not languages:
            text_parts.append("<i>Языки не добавлены</i>")
        
        text_parts.append("\n все аккаунты готовы к работе")
        
        await call.message.edit_text(
            "\n".join(text_parts),
            parse_mode='HTML',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка статистики по языкам: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)


# ОБРАБОТКА ПОСТОВ КАНАЛОВ
BOT_START_TIME = time.time()

@main_router.channel_post(F.chat.type == ChatType.CHANNEL)
async def handle_channel_post(message: Message):
    """Обработка новых постов в каналах - создание задач просмотра"""
    try:
        # ЗАЩИТА: Игнорируем старые посты (старше 5 минут)
        if message.date:
            message_time = message.date.timestamp()
            current_time = time.time()
            
            # Если пост старше 5 минут или был создан до запуска бота
            if (current_time - message_time > 300) or (message_time < BOT_START_TIME):
                logger.debug(f"⏭️ Пропускаю старый пост от {message.date} в @{message.chat.username}")
                return
        
        channel_username = message.chat.username
        if not channel_username:
            logger.warning("⛔ Пост в канале без username")
            return
        
        post_id = message.message_id
        
        logger.info(f"📝 НОВЫЙ пост в @{channel_username}, ID: {post_id}")
        
        # Создаем задачи просмотра 
        results = await task_service.create_view_tasks_for_post(
            channel_username, post_id
        )
        
        if results['total_tasks'] > 0:
            logger.info(f"""
✅ Задачи просмотра созданы для НОВОГО поста:
   📺 Канал: @{channel_username}
   📝 Пост ID: {post_id}
   📱 Задач: {results['total_tasks']}
   🌐 Языков: {results['languages']}
   ⚡ Режим: смешанные батчи
            """)
        else:
            logger.warning(f"⚠️ Не создано задач для @{channel_username} (возможно канал не в БД)")
        
    except Exception as e:
        logger.error(f"💥 Ошибка обработки поста: {e}")

# ДОБАВЛЕНИЕ ЯЗЫКА
@lang_router.callback_query(F.data == 'add_language')
async def add_language_menu(call: CallbackQuery):
    """Меню добавления языка"""
    try:
        from config import load_languages
        langs_data = load_languages()
        
        keyboard = IKBuilder()
        
        # Добавляем языки парами
        ru_langs = langs_data['ru']
        for i in range(0, len(ru_langs), 2):
            row = []
            for j in range(2):
                if i + j < len(ru_langs):
                    lang = ru_langs[i + j]
                    row.append(IKB(text=lang, callback_data=f'add_lang:{lang}'))
            keyboard.row(*row)
        
        keyboard.row(IKB(text='🔙 НАЗАД', callback_data='languages'))
        
        await call.message.edit_text(
            "<b>➕ ДОБАВЛЕНИЕ ЯЗЫКА</b>\n\n"
            "Выберите язык из списка:\n\n"
            "⚡ В новой схеме все языки готовы к работе сразу",
            parse_mode='HTML',
            reply_markup=keyboard.as_markup()
        )
        
    except Exception as e:
        logger.error(f"Ошибка меню добавления языка: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

@lang_router.callback_query(F.data.startswith('add_lang:'))
async def add_language_process(call: CallbackQuery):
    """Обработка добавления языка"""
    try:
        lang = call.data.split(':', 1)[1]
        
        success = await add_language(lang)
        
        if success:
            await call.answer(f"✅ Язык '{lang}' добавлен!", show_alert=True)
        else:
            await call.answer(f"⚠️ Язык '{lang}' уже существует", show_alert=True)
        
        # Возвращаемся к списку языков
        await languages_menu(call)
        
    except Exception as e:
        logger.error(f"Ошибка добавления языка: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

# Объединяем все роутеры
def get_all_routers():
    """Возвращает все роутеры для регистрации"""
    return [
        main_router,
        lang_router, 
        account_router,
        settings_router,
        stats_router
    ]
