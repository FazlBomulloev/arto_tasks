"""
Кастомные исключения для телеграм бота
"""

class BotBaseException(Exception):
    """Базовое исключение для бота"""
    pass

class DatabaseError(BotBaseException):
    """Ошибки базы данных"""
    pass

class SessionError(BotBaseException):
    """Ошибки работы с сессиями"""
    pass

class InvalidSessionError(SessionError):
    """Невалидная сессия"""
    pass

class AuthorizationError(SessionError):
    """Ошибка авторизации"""
    pass

class AccountValidationError(BotBaseException):
    """Ошибка валидации аккаунта"""
    pass

class TaskProcessingError(BotBaseException):
    """Ошибка обработки задач"""
    pass

class RedisError(BotBaseException):
    """Ошибки Redis"""
    pass

class FileProcessingError(BotBaseException):
    """Ошибки обработки файлов"""
    pass

class ConfigurationError(BotBaseException):
    """Ошибки конфигурации"""
    pass

class RateLimitError(BotBaseException):
    """Превышение лимитов API"""
    def __init__(self, message: str, retry_after: int = 0):
        super().__init__(message)
        self.retry_after = retry_after

class TelegramAPIError(BotBaseException):
    """Ошибки Telegram API"""
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.error_code = error_code