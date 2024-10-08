from bot.dao.base import BaseDAO
from bot.users.models import User


class UserDAO(BaseDAO):
    model = User
