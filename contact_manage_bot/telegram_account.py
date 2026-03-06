import asyncio
import random
from dataclasses import dataclass

from telethon import TelegramClient
from telethon import functions
from telethon import types
from telethon.errors import AuthRestartError
from telethon.errors import FloodWaitError
from telethon.errors import PasswordHashInvalidError
from telethon.errors import PhoneCodeExpiredError
from telethon.errors import PhoneCodeInvalidError
from telethon.errors import PhoneNumberInvalidError
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

from .config import Settings
from .sheets import ContactRow


@dataclass
class BatchResult:
    start_index: int
    next_index: int
    processed: int
    imported: int
    failed: int
    skipped: int
    imported_user_ids: list[tuple[int, str, str, str]]  # (tg_user_id, first_name, phone, username)


@dataclass
class LoginRequest:
    phone: str
    phone_code_hash: str
    session_string: str


@dataclass
class LoginResult:
    requires_password: bool
    phone: str
    session_string: str
    telegram_user_id: int
    username: str


class LoginFlowError(RuntimeError):
    def __init__(self, message: str, *, requires_new_code: bool = False) -> None:
        super().__init__(message)
        self.requires_new_code = requires_new_code


class TelegramGateway:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _build_client(self, session_string: str = "") -> TelegramClient:
        return TelegramClient(
            StringSession(session_string),
            self._settings.tg_api_id,
            self._settings.tg_api_hash,
            device_model="Contact Manage Bot",
            system_version="Ubuntu 24.04",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
        )

    async def begin_login(self, phone: str) -> LoginRequest:
        client = self._build_client()
        await client.connect()
        try:
            try:
                code = await client.send_code_request(phone)
            except PhoneNumberInvalidError as error:
                raise LoginFlowError(
                    "Неверный номер телефона. Используйте международный формат, например +79991234567."
                ) from error
            except AuthRestartError as error:
                raise LoginFlowError(
                    "Telegram попросил заново начать авторизацию. Нажмите кнопку подключения еще раз и запросите новый код."
                ) from error
            return LoginRequest(
                phone=phone,
                phone_code_hash=code.phone_code_hash,
                session_string=client.session.save(),
            )
        finally:
            await client.disconnect()

    async def complete_login(
        self,
        phone: str,
        code: str,
        phone_code_hash: str,
        session_string: str,
    ) -> LoginResult:
        client = self._build_client(session_string)
        await client.connect()
        try:
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash,
                )
            except SessionPasswordNeededError:
                return LoginResult(
                    requires_password=True,
                    phone=phone,
                    session_string=client.session.save(),
                    telegram_user_id=0,
                    username="",
                )
            except PhoneCodeExpiredError as error:
                raise LoginFlowError(
                    "Код подтверждения устарел или был сброшен Telegram. Запросите новый код и используйте только самый последний.",
                    requires_new_code=True,
                ) from error
            except PhoneCodeInvalidError as error:
                raise LoginFlowError(
                    "Код подтверждения неверный. Проверьте цифры и используйте только последний отправленный код.",
                    requires_new_code=True,
                ) from error
            except AuthRestartError as error:
                raise LoginFlowError(
                    "Telegram отклонил эту попытку входа. Нажмите кнопку подключения заново и запросите новый код. Если код пришел в Telegram, не запрашивайте второй код подряд.",
                    requires_new_code=True,
                ) from error

            me = await client.get_me()
            return LoginResult(
                requires_password=False,
                phone=phone,
                session_string=client.session.save(),
                telegram_user_id=me.id,
                username=me.username or "",
            )
        finally:
            await client.disconnect()

    async def complete_password(
        self,
        phone: str,
        password: str,
        session_string: str,
    ) -> LoginResult:
        client = self._build_client(session_string)
        await client.connect()
        try:
            try:
                await client.sign_in(password=password)
            except PasswordHashInvalidError as error:
                raise LoginFlowError("Неверный пароль двухфакторной защиты. Попробуйте еще раз.") from error
            me = await client.get_me()
            return LoginResult(
                requires_password=False,
                phone=phone,
                session_string=client.session.save(),
                telegram_user_id=me.id,
                username=me.username or "",
            )
        finally:
            await client.disconnect()

    async def verify_session(self, session_string: str) -> tuple[int, str]:
        client = self._build_client(session_string)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise RuntimeError("Telegram session is not authorized")
            me = await client.get_me()
            return me.id, me.username or ""
        finally:
            await client.disconnect()


class TelegramContactManager:
    def __init__(self, settings: Settings, session_string: str) -> None:
        self._settings = settings
        self._client = TelegramClient(
            StringSession(session_string),
            settings.tg_api_id,
            settings.tg_api_hash,
            device_model="Contact Manage Bot",
            system_version="Ubuntu 24.04",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
        )

    async def connect(self) -> None:
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError("Telegram session is not authorized")

    async def disconnect(self) -> None:
        await self._client.disconnect()

    async def _import_by_phone(self, contact: ContactRow) -> int | None:
        """Returns the telegram user id if contact was imported, or None."""
        input_contact = types.InputPhoneContact(
            client_id=random.randint(1, 2_147_483_647),
            phone=contact.phone,
            first_name=contact.first_name or "Unknown",
            last_name="",
        )
        result = await self._client(functions.contacts.ImportContactsRequest([input_contact]))
        if result.imported:
            return result.imported[0].user_id
        if result.users:
            return result.users[0].id
        return None

    async def _import_by_username(self, contact: ContactRow) -> int | None:
        """Returns the telegram user id if contact was added, or None."""
        resolved = await self._client(
            functions.contacts.ResolveUsernameRequest(contact.username)
        )
        if not resolved.users:
            return None

        user = resolved.users[0]
        first_name = contact.first_name or user.first_name or "Unknown"
        last_name = user.last_name or ""

        await self._client(
            functions.contacts.AddContactRequest(
                id=user,
                first_name=first_name,
                last_name=last_name,
                phone="",
                add_phone_privacy_exception=False,
            )
        )
        return user.id

    async def import_batch(
        self,
        contacts: list[ContactRow],
        start_index: int,
        batch_size: int,
        delay_sec: float,
        jitter_sec: float,
    ) -> BatchResult:
        chunk = contacts[start_index : start_index + batch_size]
        imported = 0
        failed = 0
        skipped = 0
        imported_user_ids: list[tuple[int, str, str, str]] = []

        for contact in chunk:
            while True:
                try:
                    if contact.phone:
                        uid = await self._import_by_phone(contact)
                    elif contact.username:
                        uid = await self._import_by_username(contact)
                    else:
                        uid = None

                    if uid is not None:
                        imported += 1
                        imported_user_ids.append(
                            (uid, contact.first_name, contact.phone, contact.username)
                        )
                    else:
                        skipped += 1
                    break
                except FloodWaitError as error:
                    await asyncio.sleep(
                        max(0.0, float(error.seconds) + random.uniform(0.0, jitter_sec))
                    )
                except Exception:
                    failed += 1
                    break

            await asyncio.sleep(max(0.0, delay_sec + random.uniform(0.0, jitter_sec)))

        processed = len(chunk)

        return BatchResult(
            start_index=start_index,
            next_index=start_index + processed,
            processed=processed,
            imported=imported,
            failed=failed,
            skipped=skipped,
            imported_user_ids=imported_user_ids,
        )

    async def get_contacts(self) -> list[types.User]:
        """Fetch all contacts from the Telegram account."""
        result = await self._client(functions.contacts.GetContactsRequest(hash=0))
        if hasattr(result, 'users'):
            return result.users
        return []

    async def delete_contacts_by_ids(self, user_ids: set[int]) -> int:
        """Delete only the contacts whose Telegram user IDs are in user_ids."""
        all_contacts = await self.get_contacts()
        to_delete = [
            types.InputUser(user_id=u.id, access_hash=u.access_hash)
            for u in all_contacts
            if u.id in user_ids
        ]
        if not to_delete:
            return 0
        # Telegram accepts up to 100 per request
        deleted = 0
        for i in range(0, len(to_delete), 100):
            batch = to_delete[i : i + 100]
            await self._client(functions.contacts.DeleteContactsRequest(id=batch))
            deleted += len(batch)
        return deleted

    async def delete_all_contacts(self) -> int:
        """Delete ALL contacts (legacy fallback)."""
        all_contacts = await self.get_contacts()
        if not all_contacts:
            return 0
        to_delete = [
            types.InputUser(user_id=u.id, access_hash=u.access_hash)
            for u in all_contacts
        ]
        deleted = 0
        for i in range(0, len(to_delete), 100):
            batch = to_delete[i : i + 100]
            await self._client(functions.contacts.DeleteContactsRequest(id=batch))
            deleted += len(batch)
        return deleted
