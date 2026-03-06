import asyncio
import io
import logging
import re
import time

from aiogram import Bot
from aiogram import Dispatcher
from aiogram import F
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.fsm.state import StatesGroup
from aiogram.types import CallbackQuery
from aiogram.types import InlineKeyboardButton
from aiogram.types import InlineKeyboardMarkup
from aiogram.types import Message
from aiogram.types import BotCommand
from aiogram.types import BufferedInputFile
from aiogram.types import User

from contact_manage_bot.config import Settings
from contact_manage_bot.config import load_settings
from contact_manage_bot.encryption import SessionCipher
from contact_manage_bot.sheets import load_contacts
from contact_manage_bot.sheets import parse_google_sheet_input
from contact_manage_bot.sheets import validate_google_source
from contact_manage_bot.sheets import validate_yandex_source
from contact_manage_bot.storage import SourceConfig
from contact_manage_bot.storage import Storage
from contact_manage_bot.telegram_account import TelegramContactManager
from contact_manage_bot.telegram_account import TelegramGateway
from contact_manage_bot.telegram_account import LoginFlowError


logging.basicConfig(level=logging.INFO)


class BotStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()
    waiting_google = State()
    waiting_yandex = State()


settings: Settings = load_settings()
storage = Storage(settings.database_path, SessionCipher(settings.session_secret))
gateway = TelegramGateway(settings)
user_locks: dict[int, asyncio.Lock] = {}
window_messages: dict[int, tuple[int, int]] = {}


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            # --- Шаг 1: Аккаунт ---
            [InlineKeyboardButton(text="— Шаг 1: Аккаунт —", callback_data="noop")],
            [
                InlineKeyboardButton(text="🔗 Подключить", callback_data="connect_account"),
                InlineKeyboardButton(text="✅ Проверить", callback_data="check_account_status"),
            ],
            # --- Шаг 2: Источник контактов ---
            [InlineKeyboardButton(text="— Шаг 2: Источник —", callback_data="noop")],
            [
                InlineKeyboardButton(text="📊 Google Sheet", callback_data="set_google"),
                InlineKeyboardButton(text="📋 Yandex CSV", callback_data="set_yandex"),
            ],
            [
                InlineKeyboardButton(text="▶ Google", callback_data="use_google"),
                InlineKeyboardButton(text="▶ Yandex", callback_data="use_yandex"),
            ],
            [InlineKeyboardButton(text="📎 Пример CSV", callback_data="show_examples")],
            # --- Шаг 3: Импорт ---
            [InlineKeyboardButton(text="— Шаг 3: Импорт —", callback_data="noop")],
            [InlineKeyboardButton(text="📥 Импорт следующих 200", callback_data="import_200")],
            [
                InlineKeyboardButton(text="🔄 Сбросить прогресс", callback_data="reset_progress"),
                InlineKeyboardButton(text="🗑 Удалить контакты", callback_data="delete_all_contacts"),
            ],
            # --- Прочее ---
            [
                InlineKeyboardButton(text="🔃 Обновить", callback_data="refresh_status"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_step"),
            ],
        ]
    )


def _prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_main")],
        ]
    )


def _is_allowed(telegram_user_id: int) -> bool:
    return settings.bot_owner_id == 0 or telegram_user_id == settings.bot_owner_id


def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]


def _format_viewer(viewer: User) -> str:
    username = f"@{viewer.username}" if viewer.username else "не указан"
    full_name = viewer.full_name.strip() or "Без имени"
    return (
        f"Имя: {full_name}\n"
        f"Username: {username}\n"
        f"Telegram user id: {viewer.id}"
    )


def _examples_text() -> str:
    return (
        "Пример CSV отправлен отдельным файлом.\n\n"
        "Что делать дальше:\n"
        "1. Скачайте файл contacts_example.csv\n"
        "2. Заполните строки своими контактами\n"
        "3. Для Google Sheets загрузите CSV в таблицу\n"
        "4. Для Yandex используйте публичную CSV-ссылку на файл с таким же форматом\n\n"
        "Обязательный заголовок первой строки:\n"
        "name,nickname,phone\n\n"
        "Google Sheets:\n"
        "1. Нажмите 'Настроить Google Sheet'\n"
        "2. Отправьте ID таблицы или ссылку\n"
        "3. На второй строке можно указать имя листа\n\n"
        "Пример:\n"
        "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890/edit#gid=0\n"
        "Sheet1\n\n"
        "Yandex CSV:\n"
        "1. Нажмите 'Настроить Yandex CSV'\n"
        "2. Отправьте публичную CSV-ссылку\n\n"
        "Пример:\n"
        "https://example.com/export.csv"
    )


def _example_csv_file() -> BufferedInputFile:
    buffer = io.StringIO(newline="")
    buffer.write("name,nickname,phone\r\n")
    buffer.write("Иван Иванов,,+79991234567\r\n")
    buffer.write("Петр,@petya,\r\n")
    buffer.write("Анна,anna_support,+12025550123\r\n")
    return BufferedInputFile(
        file=buffer.getvalue().encode("utf-8-sig"),
        filename="contacts_example.csv",
    )


async def _build_status(viewer: User) -> str:
    user_id = viewer.id
    account = await storage.get_account(user_id)
    source = await storage.get_source(user_id)
    pending = await storage.get_pending_auth(user_id)

    viewer_status = _format_viewer(viewer)

    account_status = "не подключен"
    if account is not None:
        username = f"@{account.username}" if account.username else "не указан"
        account_status = (
            f"подключен\n"
            f"Телефон: {account.phone}\n"
            f"Telegram user id: {account.telegram_user_id}\n"
            f"Имя пользователя: {username}"
        )

    auth_status = "не активен"
    if pending is not None:
        auth_status = f"ожидается код или пароль для {pending.phone}"

    active_source_labels = {
        "google": "Google Sheets",
        "yandex_csv": "Yandex CSV",
    }
    active_source = active_source_labels.get(source.active_source or "", "не выбран")
    google_status = source.google_sheet_id or "не настроен"
    yandex_status = source.yandex_csv_url or "не настроен"

    return (
        "📋 Панель управления\n\n"
        f"👤 Пользователь:\n{viewer_status}\n\n"
        f"🔗 Аккаунт: {account_status}\n\n"
        f"🔑 Авторизация: {auth_status}\n\n"
        f"📂 Источник: {active_source}\n"
        f"   Google: {google_status} / {source.google_worksheet}\n"
        f"   Yandex: {yandex_status}\n\n"
        f"📥 Импортировано до индекса: {source.next_index}"
    )


async def _safe_delete_message(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramBadRequest:
        return


async def _delete_user_message(message: Message) -> None:
    try:
        await message.delete()
    except TelegramBadRequest:
        return


async def _render_window(
    bot: Bot,
    chat_id: int,
    user_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup,
    preferred_message_id: int | None = None,
) -> None:
    known_window = window_messages.get(user_id)
    target_message_id = preferred_message_id
    if target_message_id is None and known_window is not None and known_window[0] == chat_id:
        target_message_id = known_window[1]

    if target_message_id is not None:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=target_message_id,
                text=text,
                reply_markup=reply_markup,
            )
            window_messages[user_id] = (chat_id, target_message_id)
            return
        except TelegramBadRequest:
            pass

    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    if known_window is not None and known_window[0] == chat_id:
        await _safe_delete_message(bot, chat_id, known_window[1])
    window_messages[user_id] = (chat_id, sent.message_id)


async def _show_dashboard(
    bot: Bot,
    chat_id: int,
    viewer: User,
    preferred_message_id: int | None = None,
    notice: str | None = None,
) -> None:
    status_text = await _build_status(viewer)
    if notice:
        status_text = f"{notice}\n\n{status_text}"
    await _render_window(
        bot=bot,
        chat_id=chat_id,
        user_id=viewer.id,
        text=status_text,
        reply_markup=_main_keyboard(),
        preferred_message_id=preferred_message_id,
    )


async def _show_prompt(
    bot: Bot,
    chat_id: int,
    user_id: int,
    text: str,
    preferred_message_id: int | None = None,
) -> None:
    await _render_window(
        bot=bot,
        chat_id=chat_id,
        user_id=user_id,
        text=text,
        reply_markup=_prompt_keyboard(),
        preferred_message_id=preferred_message_id,
    )


async def _verify_saved_account(user_id: int) -> tuple[int, str]:
    account = await storage.get_account(user_id)
    if account is None:
        raise RuntimeError("Telegram-аккаунт не подключен")

    telegram_user_id, username = await gateway.verify_session(account.session_string)
    await storage.save_account(
        user_id,
        account.phone,
        telegram_user_id,
        username,
        account.session_string,
    )
    return telegram_user_id, username


async def _ensure_private_chat(target: Message | CallbackQuery) -> bool:
    if isinstance(target, CallbackQuery):
        chat = target.message.chat
        if chat.type == "private":
            return True
        await target.answer("Используйте бота только в личном чате", show_alert=True)
        return False

    if target.chat.type == "private":
        return True
    await target.answer("Используйте бота только в личном чате.")
    return False


async def _ensure_access(actor_id: int, target: Message | CallbackQuery) -> bool:
    if not await _ensure_private_chat(target):
        return False

    if _is_allowed(actor_id):
        return True

    if isinstance(target, CallbackQuery):
        await target.answer("Доступ запрещен", show_alert=True)
    else:
        await target.answer("Доступ запрещен.")
    return False


async def _create_manager(user_id: int) -> TelegramContactManager:
    account = await storage.get_account(user_id)
    if account is None:
        raise RuntimeError("Telegram-аккаунт не подключен")

    manager = TelegramContactManager(settings, account.session_string)
    await manager.connect()
    return manager


def _remaining_cooldown(last_event_ts: int | None, cooldown_sec: int) -> int:
    if last_event_ts is None:
        return 0
    elapsed = int(time.time()) - last_event_ts
    return max(0, cooldown_sec - elapsed)


async def on_start(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await state.clear()
    await _delete_user_message(message)
    await _show_dashboard(message.bot, message.chat.id, message.from_user)


async def on_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await state.clear()
    await callback.answer("Отменено")
    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
        notice="Текущий шаг отменен.",
    )


async def on_refresh(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await callback.answer()
    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
    )


async def on_connect_account(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await state.set_state(BotStates.waiting_phone)
    await callback.answer()
    await _show_prompt(
        callback.bot,
        callback.message.chat.id,
        callback.from_user.id,
        "Отправьте номер Telegram в международном формате, например +15551234567.",
        preferred_message_id=callback.message.message_id,
    )


async def on_set_google(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await state.set_state(BotStates.waiting_google)
    await callback.answer()
    await _show_prompt(
        callback.bot,
        callback.message.chat.id,
        callback.from_user.id,
        "Отправьте Google Sheet ID или полную ссылку на Google Sheets. При необходимости укажите имя листа на второй строке.",
        preferred_message_id=callback.message.message_id,
    )


async def on_set_yandex(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await state.set_state(BotStates.waiting_yandex)
    await callback.answer()
    await _show_prompt(
        callback.bot,
        callback.message.chat.id,
        callback.from_user.id,
        "Отправьте публичную ссылку Yandex CSV.",
        preferred_message_id=callback.message.message_id,
    )


async def on_use_google(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    source = await storage.get_source(callback.from_user.id)
    if not source.google_sheet_id:
        await callback.answer("Google Sheet еще не настроен", show_alert=True)
        return

    await storage.set_active_source(callback.from_user.id, "google")
    await callback.answer("Источник Google выбран")
    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
        notice="Источник Google выбран.",
    )


async def on_check_account_status(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await callback.answer()
    try:
        telegram_user_id, username = await _verify_saved_account(callback.from_user.id)
        username_text = f"@{username}" if username else "не указан"
        notice = (
            "Проверка прошла успешно. Сессия Telegram активна.\n"
            f"Telegram user id: {telegram_user_id}\n"
            f"Имя пользователя: {username_text}"
        )
    except Exception as error:
        notice = (
            "Проверка не пройдена. Сохраненная сессия сейчас недоступна.\n"
            f"Причина: {error}\n"
            "Подключите Telegram-аккаунт заново."
        )

    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
        notice=notice,
    )


async def on_use_yandex(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    source = await storage.get_source(callback.from_user.id)
    if not source.yandex_csv_url:
        await callback.answer("Yandex CSV еще не настроен", show_alert=True)
        return

    await storage.set_active_source(callback.from_user.id, "yandex_csv")
    await callback.answer("Источник Yandex выбран")
    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
        notice="Источник Yandex выбран.",
    )


async def on_reset_progress(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await storage.reset_next_index(callback.from_user.id)
    await callback.answer("Прогресс сброшен")
    await _show_dashboard(
        callback.bot,
        callback.message.chat.id,
        callback.from_user,
        preferred_message_id=callback.message.message_id,
        notice="Прогресс сброшен.",
    )


async def on_phone(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await _delete_user_message(message)

    login_stats = await storage.get_operation_stats(
        message.from_user.id,
        "login_code",
        settings.login_code_cooldown_sec,
    )
    cooldown_left = _remaining_cooldown(
        login_stats.last_event_ts,
        settings.login_code_cooldown_sec,
    )
    if cooldown_left > 0:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Слишком много запросов кода входа. Подождите {cooldown_left} сек. перед новой попыткой.",
        )
        return

    phone = (message.text or "").strip()
    try:
        request = await gateway.begin_login(phone)
    except Exception as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Не удалось отправить код входа: {error}",
        )
        return

    await storage.save_pending_auth(
        message.from_user.id,
        request.phone,
        request.phone_code_hash,
        request.session_string,
    )
    await storage.log_action(message.from_user.id, "login_code", 1)
    await state.set_state(BotStates.waiting_code)
    await _show_prompt(
        message.bot,
        message.chat.id,
        message.from_user.id,
        "Telegram отправил код входа.\n\n"
        "ВАЖНО: НЕ отправляйте код как есть!\n"
        "Telegram заблокирует вход, если код будет отправлен в чат целиком.\n\n"
        "Разделите цифры пробелами или точками, например:\n"
        "1 2 3 4 5\n"
        "или\n"
        "1.2.3.4.5\n\n"
        "Если включена двухфакторная защита, следующим шагом я попрошу пароль.",
    )


async def on_code(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await _delete_user_message(message)

    pending = await storage.get_pending_auth(message.from_user.id)
    if pending is None:
        await state.clear()
        await _show_dashboard(
            message.bot,
            message.chat.id,
            message.from_user,
            notice="Нет активного запроса на вход. Начните с кнопки подключения Telegram-аккаунта.",
        )
        return

    code = re.sub(r"[^0-9]", "", message.text or "")
    try:
        result = await gateway.complete_login(
            phone=pending.phone,
            code=code,
            phone_code_hash=pending.phone_code_hash,
            session_string=pending.session_string,
        )
    except LoginFlowError as error:
        if error.requires_new_code:
            await storage.clear_pending_auth(message.from_user.id)
            await state.clear()
            await _show_dashboard(
                message.bot,
                message.chat.id,
                message.from_user,
                notice=(
                    f"{error}\n"
                    "Запросите новый код кнопкой подключения аккаунта и используйте только последний код из Telegram."
                ),
            )
            return

        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            str(error),
        )
        return
    except Exception as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Не удалось завершить вход: {error}",
        )
        return

    if result.requires_password:
        await storage.save_pending_auth(
            message.from_user.id,
            result.phone,
            pending.phone_code_hash,
            result.session_string,
        )
        await state.set_state(BotStates.waiting_password)
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            "Включена двухфакторная защита. Отправьте пароль Telegram Cloud.",
        )
        return

    await storage.save_account(
        message.from_user.id,
        result.phone,
        result.telegram_user_id,
        result.username,
        result.session_string,
    )
    await _verify_saved_account(message.from_user.id)
    await storage.clear_pending_auth(message.from_user.id)
    await state.clear()
    await _show_dashboard(
        message.bot,
        message.chat.id,
        message.from_user,
        notice="Telegram-аккаунт подключен и успешно проверен.",
    )


async def on_password(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await _delete_user_message(message)

    pending = await storage.get_pending_auth(message.from_user.id)
    if pending is None:
        await state.clear()
        await _show_dashboard(
            message.bot,
            message.chat.id,
            message.from_user,
            notice="Нет активного запроса пароля. Начните с кнопки подключения Telegram-аккаунта.",
        )
        return

    try:
        result = await gateway.complete_password(
            phone=pending.phone,
            password=(message.text or "").strip(),
            session_string=pending.session_string,
        )
    except LoginFlowError as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            str(error),
        )
        return
    except Exception as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Не удалось проверить пароль: {error}",
        )
        return

    await storage.save_account(
        message.from_user.id,
        result.phone,
        result.telegram_user_id,
        result.username,
        result.session_string,
    )
    await _verify_saved_account(message.from_user.id)
    await storage.clear_pending_auth(message.from_user.id)
    await state.clear()
    await _show_dashboard(
        message.bot,
        message.chat.id,
        message.from_user,
        notice="Telegram-аккаунт подключен и успешно проверен.",
    )


async def on_google_details(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await _delete_user_message(message)

    lines = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
    if not lines:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            "Отправьте Google Sheet ID или ссылку. На второй строке можно указать имя листа.\n\nПример:\nhttps://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890/edit#gid=0\nSheet1",
        )
        return

    try:
        sheet_id = parse_google_sheet_input(lines[0])
    except ValueError as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            str(error),
        )
        return

    worksheet = lines[1] if len(lines) > 1 else "Sheet1"
    try:
        row_count = await validate_google_source(settings, sheet_id, worksheet)
    except Exception as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Не удалось проверить Google Sheet: {error}\n\nПроверьте ссылку, имя листа, доступ сервисного аккаунта и заголовок name,nickname,phone.",
        )
        return

    await storage.save_google_source(message.from_user.id, sheet_id, worksheet)
    await storage.set_active_source(message.from_user.id, "google")
    await storage.reset_next_index(message.from_user.id)
    await state.clear()
    await _show_dashboard(
        message.bot,
        message.chat.id,
        message.from_user,
        notice=(
            "Источник Google Sheet сохранен и выбран. "
            f"Найдено строк для импорта: {row_count}. Прогресс сброшен на 0."
        ),
    )


async def on_yandex_details(message: Message, state: FSMContext) -> None:
    if not await _ensure_access(message.from_user.id, message):
        return

    await _delete_user_message(message)

    csv_url = (message.text or "").strip()
    if not csv_url.startswith("http://") and not csv_url.startswith("https://"):
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            "Отправьте корректную публичную ссылку http или https на CSV.\n\nПример:\nhttps://example.com/export.csv",
        )
        return

    try:
        row_count = await validate_yandex_source(csv_url)
    except Exception as error:
        await _show_prompt(
            message.bot,
            message.chat.id,
            message.from_user.id,
            f"Не удалось проверить Yandex CSV: {error}\n\nПроверьте публичную CSV-ссылку и заголовок name,nickname,phone.",
        )
        return

    await storage.save_yandex_source(message.from_user.id, csv_url)
    await storage.set_active_source(message.from_user.id, "yandex_csv")
    await storage.reset_next_index(message.from_user.id)
    await state.clear()
    await _show_dashboard(
        message.bot,
        message.chat.id,
        message.from_user,
        notice=(
            "Источник Yandex CSV сохранен и выбран. "
            f"Найдено строк для импорта: {row_count}. Прогресс сброшен на 0."
        ),
    )


async def on_show_examples(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    await callback.answer()
    await callback.bot.send_document(
        chat_id=callback.message.chat.id,
        document=_example_csv_file(),
        caption="Пример CSV для импорта контактов.",
    )
    await _show_prompt(
        callback.bot,
        callback.message.chat.id,
        callback.from_user.id,
        _examples_text(),
        preferred_message_id=callback.message.message_id,
    )


async def _load_ready_source(user_id: int) -> SourceConfig:
    source = await storage.get_source(user_id)
    if not source.active_source:
        raise RuntimeError("Активный источник не выбран")
    return source


async def on_import(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    lock = _get_user_lock(callback.from_user.id)
    if lock.locked():
        await callback.answer("Сейчас уже выполняется другая операция", show_alert=True)
        return

    async with lock:
        await callback.answer()
        await _show_prompt(
            callback.bot,
            callback.message.chat.id,
            callback.from_user.id,
            "Импорт запущен. Бот будет ждать лимиты Telegram и продолжит работу, пока текущая пачка не завершится.",
            preferred_message_id=callback.message.message_id,
        )
        manager: TelegramContactManager | None = None

        try:
            cooldown_stats = await storage.get_operation_stats(
                callback.from_user.id,
                "import_batch",
                settings.import_cooldown_sec,
            )
            cooldown_left = _remaining_cooldown(
                cooldown_stats.last_event_ts,
                settings.import_cooldown_sec,
            )
            if cooldown_left > 0:
                await _show_dashboard(
                    callback.bot,
                    callback.message.chat.id,
                    callback.from_user,
                    preferred_message_id=callback.message.message_id,
                    notice=(
                        f"Сейчас действует пауза между импортами. Подождите {cooldown_left} сек. перед следующим запуском."
                    ),
                )
                return

            source = await _load_ready_source(callback.from_user.id)
            contacts = await load_contacts(settings, source)
            manager = await _create_manager(callback.from_user.id)

            if source.next_index >= len(contacts):
                await _show_dashboard(
                    callback.bot,
                    callback.message.chat.id,
                    callback.from_user,
                    preferred_message_id=callback.message.message_id,
                    notice="Свободных строк больше нет. Используйте сброс прогресса, если хотите начать сначала.",
                )
                return

            safe_batch_size = min(
                settings.batch_size,
                settings.max_batch_size,
                200,
            )
            if safe_batch_size <= 0:
                await _show_dashboard(
                    callback.bot,
                    callback.message.chat.id,
                    callback.from_user,
                    preferred_message_id=callback.message.message_id,
                    notice="Импорт заблокирован текущими ограничениями безопасности.",
                )
                return

            result = await manager.import_batch(
                contacts=contacts,
                start_index=source.next_index,
                batch_size=safe_batch_size,
                delay_sec=settings.sleep_between_requests_sec,
                jitter_sec=settings.request_jitter_sec,
            )
            await storage.set_next_index(callback.from_user.id, result.next_index)
            await storage.log_action(callback.from_user.id, "import_batch", result.processed)
            await storage.log_action(callback.from_user.id, "import_contact", result.processed)

            notice = (
                "Импорт завершен.\n"
                f"Стартовый индекс: {result.start_index}\n"
                f"Обработано: {result.processed}\n"
                f"Импортировано: {result.imported}\n"
                f"Ошибок: {result.failed}\n"
                f"Пропущено: {result.skipped}\n"
                f"Следующий индекс: {result.next_index}"
            )
            await _show_dashboard(
                callback.bot,
                callback.message.chat.id,
                callback.from_user,
                preferred_message_id=callback.message.message_id,
                notice=notice,
            )
        except Exception as error:
            await _show_dashboard(
                callback.bot,
                callback.message.chat.id,
                callback.from_user,
                preferred_message_id=callback.message.message_id,
                notice=f"Не удалось запустить импорт: {error}",
            )
        finally:
            if manager is not None:
                await manager.disconnect()


async def on_delete_all(callback: CallbackQuery) -> None:
    if not await _ensure_access(callback.from_user.id, callback):
        return

    lock = _get_user_lock(callback.from_user.id)
    if lock.locked():
        await callback.answer("Сейчас уже выполняется другая операция", show_alert=True)
        return

    async with lock:
        await callback.answer()
        manager: TelegramContactManager | None = None
        try:
            delete_stats = await storage.get_operation_stats(
                callback.from_user.id,
                "delete_contacts",
                settings.delete_cooldown_sec,
            )
            cooldown_left = _remaining_cooldown(
                delete_stats.last_event_ts,
                settings.delete_cooldown_sec,
            )
            if cooldown_left > 0:
                await _show_dashboard(
                    callback.bot,
                    callback.message.chat.id,
                    callback.from_user,
                    preferred_message_id=callback.message.message_id,
                    notice=(
                        f"Сейчас действует пауза между удалениями. Подождите {cooldown_left} сек. перед повторным удалением контактов."
                    ),
                )
                return

            manager = await _create_manager(callback.from_user.id)
            await _show_prompt(
                callback.bot,
                callback.message.chat.id,
                callback.from_user.id,
                "Удаляю Telegram-контакты из облака...",
                preferred_message_id=callback.message.message_id,
            )
            deleted_count = await manager.delete_all_contacts()
            await storage.log_action(callback.from_user.id, "delete_contacts", deleted_count)
            await _show_dashboard(
                callback.bot,
                callback.message.chat.id,
                callback.from_user,
                preferred_message_id=callback.message.message_id,
                notice=f"Удалено контактов: {deleted_count}",
            )
        except Exception as error:
            await _show_dashboard(
                callback.bot,
                callback.message.chat.id,
                callback.from_user,
                preferred_message_id=callback.message.message_id,
                notice=f"Не удалось удалить контакты: {error}",
            )
        finally:
            if manager is not None:
                await manager.disconnect()


async def _on_noop(callback: CallbackQuery) -> None:
    await callback.answer()


async def _configure_bot_profile(bot: Bot) -> None:
    await bot.set_my_description(
        "Бот для импорта контактов в Telegram из Google Sheets или Yandex CSV.\n\n"
        "Шаг 1: Подключите Telegram-аккаунт\n"
        "Шаг 2: Настройте источник контактов\n"
        "Шаг 3: Нажмите Импорт"
    )
    await bot.set_my_short_description(
        "Импорт контактов в Telegram из Google Sheets или Yandex CSV."
    )
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Открыть панель управления"),
        ]
    )


async def main() -> None:
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    bot = Bot(token=settings.bot_token)
    await _configure_bot_profile(bot)
    dispatcher = Dispatcher()

    dispatcher.message.register(on_start, CommandStart())
    dispatcher.message.register(on_phone, BotStates.waiting_phone)
    dispatcher.message.register(on_code, BotStates.waiting_code)
    dispatcher.message.register(on_password, BotStates.waiting_password)
    dispatcher.message.register(on_google_details, BotStates.waiting_google)
    dispatcher.message.register(on_yandex_details, BotStates.waiting_yandex)

    dispatcher.callback_query.register(on_connect_account, F.data == "connect_account")
    dispatcher.callback_query.register(on_set_google, F.data == "set_google")
    dispatcher.callback_query.register(on_set_yandex, F.data == "set_yandex")
    dispatcher.callback_query.register(on_check_account_status, F.data == "check_account_status")
    dispatcher.callback_query.register(on_use_google, F.data == "use_google")
    dispatcher.callback_query.register(on_use_yandex, F.data == "use_yandex")
    dispatcher.callback_query.register(on_show_examples, F.data == "show_examples")
    dispatcher.callback_query.register(on_import, F.data == "import_200")
    dispatcher.callback_query.register(on_reset_progress, F.data == "reset_progress")
    dispatcher.callback_query.register(on_delete_all, F.data == "delete_all_contacts")
    dispatcher.callback_query.register(on_refresh, F.data == "refresh_status")
    dispatcher.callback_query.register(on_cancel_callback, F.data == "cancel_step")
    dispatcher.callback_query.register(on_cancel_callback, F.data == "back_main")
    dispatcher.callback_query.register(_on_noop, F.data == "noop")

    try:
        await dispatcher.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
