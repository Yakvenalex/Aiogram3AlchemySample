from typing import List, Any, Dict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy import update as sqlalchemy_update, delete as sqlalchemy_delete, func
from loguru import logger
from bot.database import async_session_maker


class BaseDAO:
    model = None  # Устанавливается в дочернем классе

    @classmethod
    async def find_one_or_none_by_id(cls, data_id: int):
        # Найти запись по ID
        logger.info(f"Поиск {cls.model.__name__} с ID: {data_id}")
        async with async_session_maker() as session:
            try:
                query = select(cls.model).filter_by(id=data_id)
                result = await session.execute(query)
                record = result.scalar_one_or_none()
                if record:
                    logger.info(f"Запись с ID {data_id} найдена.")
                else:
                    logger.info(f"Запись с ID {data_id} не найдена.")
                return record
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при поиске записи с ID {data_id}: {e}")
                raise

    @classmethod
    async def find_one_or_none(cls, **filter_by):
        # Найти одну запись по фильтрам
        logger.info(f"Поиск одной записи {cls.model.__name__} по фильтрам: {filter_by}")
        async with async_session_maker() as session:
            try:
                query = select(cls.model).filter_by(**filter_by)
                result = await session.execute(query)
                record = result.scalar_one_or_none()
                if record:
                    logger.info(f"Запись найдена по фильтрам: {filter_by}")
                else:
                    logger.info(f"Запись не найдена по фильтрам: {filter_by}")
                return record
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при поиске записи по фильтрам {filter_by}: {e}")
                raise

    @classmethod
    async def find_all(cls, **filter_by):
        # Найти все записи по фильтрам
        logger.info(f"Поиск всех записей {cls.model.__name__} по фильтрам: {filter_by}")
        async with async_session_maker() as session:
            try:
                query = select(cls.model).filter_by(**filter_by)
                result = await session.execute(query)
                records = result.scalars().all()
                logger.info(f"Найдено {len(records)} записей.")
                return records
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при поиске всех записей по фильтрам {filter_by}: {e}")
                raise

    @classmethod
    async def add(cls, **values):
        # Добавить одну запись
        logger.info(f"Добавление записи {cls.model.__name__} с параметрами: {values}")
        async with async_session_maker() as session:
            async with session.begin():
                new_instance = cls.model(**values)
                session.add(new_instance)
                try:
                    await session.commit()
                    logger.info(f"Запись {cls.model.__name__} успешно добавлена.")
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при добавлении записи: {e}")
                    raise e
                return new_instance

    @classmethod
    async def add_many(cls, instances: list[dict]):
        # Добавить несколько записей
        logger.info(f"Добавление нескольких записей {cls.model.__name__}. Количество: {len(instances)}")
        async with async_session_maker() as session:
            async with session.begin():
                new_instances = [cls.model(**values) for values in instances]
                session.add_all(new_instances)
                try:
                    await session.commit()
                    logger.info(f"Успешно добавлено {len(new_instances)} записей.")
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при добавлении нескольких записей: {e}")
                    raise e
                return new_instances

    @classmethod
    async def update(cls, filter_by, **values):
        # Обновить записи по фильтру
        logger.info(f"Обновление записей {cls.model.__name__} по фильтру: {filter_by} с параметрами: {values}")
        async with async_session_maker() as session:
            async with session.begin():
                query = (
                    sqlalchemy_update(cls.model)
                    .where(*[getattr(cls.model, k) == v for k, v in filter_by.items()])
                    .values(**values)
                    .execution_options(synchronize_session="fetch")
                )
                try:
                    result = await session.execute(query)
                    await session.commit()
                    logger.info(f"Обновлено {result.rowcount} записей.")
                    return result.rowcount
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при обновлении записей: {e}")
                    raise e

    @classmethod
    async def delete(cls, delete_all: bool = False, **filter_by):
        # Удалить записи по фильтру
        logger.info(f"Удаление записей {cls.model.__name__} по фильтру: {filter_by}")
        if not delete_all and not filter_by:
            logger.error("Нужен хотя бы один фильтр для удаления.")
            raise ValueError("Нужен хотя бы один фильтр для удаления.")

        async with async_session_maker() as session:
            async with session.begin():
                query = sqlalchemy_delete(cls.model).filter_by(**filter_by)
                try:
                    result = await session.execute(query)
                    await session.commit()
                    logger.info(f"Удалено {result.rowcount} записей.")
                    return result.rowcount
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при удалении записей: {e}")
                    raise e

    @classmethod
    async def count(cls, **filter_by):
        # Подсчитать количество записей
        logger.info(f"Подсчет количества записей {cls.model.__name__} по фильтру: {filter_by}")
        async with async_session_maker() as session:
            try:
                query = select(func.count(cls.model.id)).filter_by(**filter_by)
                result = await session.execute(query)
                count = result.scalar()
                logger.info(f"Найдено {count} записей.")
                return count
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при подсчете записей: {e}")
                raise

    @classmethod
    async def paginate(cls, page: int = 1, page_size: int = 10, **filter_by):
        # Пагинация записей
        logger.info(
            f"Пагинация записей {cls.model.__name__} по фильтру: {filter_by}, "
            f"страница: {page}, "
            f"размер страницы: {page_size}")
        async with async_session_maker() as session:
            try:
                query = select(cls.model).filter_by(**filter_by)
                result = await session.execute(query.offset((page - 1) * page_size).limit(page_size))
                records = result.scalars().all()
                logger.info(f"Найдено {len(records)} записей на странице {page}.")
                return records
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при пагинации записей: {e}")
                raise

    @classmethod
    async def find_by_ids(cls, ids: List[int]) -> List[Any]:
        """Найти несколько записей по списку ID"""
        logger.info(f"Поиск записей {cls.model.__name__} по списку ID: {ids}")
        async with async_session_maker() as session:
            try:
                query = select(cls.model).filter(cls.model.id.in_(ids))
                result = await session.execute(query)
                records = result.scalars().all()
                logger.info(f"Найдено {len(records)} записей по списку ID.")
                return records
            except SQLAlchemyError as e:
                logger.error(f"Ошибка при поиске записей по списку ID: {e}")
                raise

    @classmethod
    async def upsert(cls, unique_fields: List[str], **values) -> Any:
        """Создать запись или обновить существующую"""
        logger.info(f"Upsert для {cls.model.__name__}")
        filter_dict = {field: values[field] for field in unique_fields if field in values}

        async with async_session_maker() as session:
            async with session.begin():
                try:
                    existing = await cls.find_one_or_none(**filter_dict)
                    if existing:
                        # Обновляем существующую запись
                        for key, value in values.items():
                            setattr(existing, key, value)
                        await session.commit()
                        logger.info(f"Обновлена существующая запись {cls.model.__name__}")
                        return existing
                    else:
                        # Создаем новую запись
                        new_instance = cls.model(**values)
                        session.add(new_instance)
                        await session.commit()
                        logger.info(f"Создана новая запись {cls.model.__name__}")
                        return new_instance
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при upsert: {e}")
                    raise

    @classmethod
    async def bulk_update(cls, records: List[Dict[str, Any]]) -> int:
        """Массовое обновление записей"""
        logger.info(f"Массовое обновление записей {cls.model.__name__}")
        async with async_session_maker() as session:
            async with session.begin():
                try:
                    updated_count = 0
                    for record in records:
                        if 'id' not in record:
                            continue

                        update_data = {k: v for k, v in record.items() if k != 'id'}
                        stmt = (
                            sqlalchemy_update(cls.model)
                            .filter_by(id=record['id'])
                            .values(**update_data)
                        )
                        result = await session.execute(stmt)
                        updated_count += result.rowcount

                    await session.commit()
                    logger.info(f"Обновлено {updated_count} записей")
                    return updated_count
                except SQLAlchemyError as e:
                    await session.rollback()
                    logger.error(f"Ошибка при массовом обновлении: {e}")
                    raise
