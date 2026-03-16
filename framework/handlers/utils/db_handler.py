from __future__ import annotations

import logging
from time import sleep
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import sqlalchemy
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy.exc import IntegrityError, NotSupportedError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text

from framework.types import RowMappings, TsdbTableColumn

logger = logging.getLogger(__name__)


class TableHandler:  # pragma: no cover
    db_string: str

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, user: str, password: str, host: str, port: str, db_name: str
    ) -> None:
        self.db_string = "postgresql+psycopg://" + user + ":" + password + "@" + host + ":" + port + "/" + db_name

    def make_table_if_not_exists(self, table_name: str, schema: Sequence[TsdbTableColumn]) -> MetaData:
        # Here you need to insert your own path to db
        engine = create_engine(self.db_string, connect_args={})
        metadata = MetaData()
        tables_list = []
        # for table in d['list']:
        # If table don't exist, Create.
        if not sqlalchemy.inspect(engine).has_table(table_name):
            column_counter = 0
            columns: List[Column] = []
            for item in schema:
                column_counter = column_counter + 1
                pkey = "pkey" in item and bool(item["pkey"])
                col_type = getattr(sqlalchemy, item["type"])
                logger.debug("Considering Column Payload: %s", item)
                columns.append(Column(item["col_name"], col_type, primary_key=pkey))
            table = Table(table_name, metadata, *columns)
            tables_list.append(table)
        else:
            logger.debug("Table already exists %s", table_name)

        logger.debug("Tables Processed: %s", str(tables_list))
        metadata.create_all(engine)  # Create table
        return metadata


class DataHandler:
    db_string: str
    session: Optional[Session] = None
    table: Optional[Table] = None
    table_name: Optional[str] = None
    column_list: List[str] = []

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, user: str, password: str, host: str, port: str, db_name: str
    ) -> None:
        self.db_string = "postgresql+psycopg://" + user + ":" + password + "@" + host + ":" + port + "/" + db_name

    def __get_column_names(self) -> List[str]:  # pragma: no cover
        if self.table is None:
            return []
        col_list = list(self.table.columns.values())
        column_list: List[str] = []
        for col in col_list:
            column_list.append(col.name)
        return column_list

    def __serialize_row(self, value: Mapping[str, Any]) -> Dict[str, Any]:
        row = value
        trimmed_row: Dict[str, Any] = {}
        for col in self.column_list:
            if col in row:
                trimmed_row[col] = row[col]
        return trimmed_row

    def set_table(self, table_name: str) -> None:  # pragma: no cover
        self.table_name = table_name
        engine = create_engine(self.db_string)
        meta = MetaData()
        meta.reflect(bind=engine)
        self.table = meta.tables[table_name]
        session = sessionmaker(bind=engine)
        self.session = session()
        self.column_list = self.__get_column_names()
        logger.info("Set Table: %s", table_name)

    def _reset_session(self) -> None:  # pragma: no cover
        if self.session is not None:
            self.session.close()
        sleep(1)
        if self.table_name is None:
            raise RuntimeError("Table name not initialized")
        self.set_table(self.table_name)

    def insert_tsdb(self, msgs: RowMappings) -> None:
        if not msgs:
            return
        logger.debug(msgs)
        if self.session is None or self.table is None:
            raise RuntimeError("Table or session not initialized")
        try:
            rows = [self.__serialize_row(m) for m in msgs]
            self.session.execute(self.table.insert(), rows)
            self.session.commit()
            logger.debug("Inserted %d msgs", len(rows))
        except IntegrityError as exc:
            self.session.rollback()
            logger.warning("Bulk insert conflict, retrying individually: %s", str(exc))
            self.insert_tsdb_linear(msgs)
        except Exception:  # pylint: disable=broad-exception-caught
            self.session.rollback()
            logger.warning("Retrying bulk insert after reconnect")
            self._reset_session()
            self.insert_tsdb(msgs)

    def insert_tsdb_linear(self, msgs: Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]) -> None:
        if not msgs:
            return
        if isinstance(msgs, Mapping):
            iterable: Sequence[Mapping[str, Any]] = [msgs]
        else:
            iterable = msgs
        logger.debug(iterable)
        if self.session is None or self.table is None:
            raise RuntimeError("Table or session not initialized")
        for msg in iterable:
            try:
                row = self.__serialize_row(msg)
                insert_statement = self.table.insert().values(row)
                self.session.execute(insert_statement)
                self.session.commit()
                logger.debug("Inserted msg")
            except NotSupportedError as exc:
                self.session.rollback()
                logger.warning("Bad payload : %s - %s", msg, str(exc))
            except IntegrityError:
                self.session.rollback()
                logger.warning("Duplicate payload : %s", msg)
            except Exception:  # pylint: disable=broad-exception-caught
                self.session.rollback()
                logger.warning("Retrying single insert after reconnect")
                self._reset_session()
                self.insert_tsdb_linear(msg)

    def query_table(self, query: str) -> List[Any]:  # pragma: no cover
        if self.session is None:
            if self.table_name is None:
                raise RuntimeError("Table name not initialized")
            self.set_table(self.table_name)
        if self.session is None:
            raise RuntimeError("Session not initialized")
        result = self.session.execute(text(query))
        logger.debug(result)
        return list(result.all())
