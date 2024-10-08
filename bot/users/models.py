from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import BigInteger

from typing import Optional
from bot.database import Base


class User(Base):
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    username: Mapped[Optional[str]]
    first_name: Mapped[Optional[str]]
    last_name: Mapped[Optional[str]]
    referral_id: Mapped[Optional[int]]