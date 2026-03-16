from __future__ import annotations

from typing import Any, Optional, cast

import pytest
from sqlalchemy.exc import IntegrityError, NotSupportedError

from framework.handlers.utils.db_handler import DataHandler


class DummyInsertBuilder:
    label = "INSERT"

    def values(self, row: Any) -> tuple[str, Any]:
        return ("INSERT", row)


class DummyTable:
    def insert(self) -> DummyInsertBuilder:
        return DummyInsertBuilder()


class RecordingSession:
    def __init__(self, behaviors: Optional[list[Exception]] = None) -> None:
        self.behaviors = behaviors or []
        self.executed: list[tuple[Any, Any]] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False
        self._call_index = 0

    def execute(self, statement: Any, params: Any = None) -> None:
        if self._call_index < len(self.behaviors):
            exc = self.behaviors[self._call_index]
            self._call_index += 1
            raise exc
        self._call_index += 1
        if isinstance(statement, DummyInsertBuilder):
            self.executed.append((statement.label, params))
        elif isinstance(statement, tuple):
            self.executed.append(statement)
        else:
            self.executed.append((statement, params))

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.closed = True


def build_handler(session: RecordingSession) -> DataHandler:
    handler = DataHandler("u", "p", "h", "5432", "db")
    handler.table_name = "tbl"
    handler.table = cast(Any, DummyTable())
    handler.session = cast(Any, session)
    handler.column_list = ["a", "b"]
    return handler


def test_insert_tsdb_bulk_success() -> None:
    session = RecordingSession()
    handler = build_handler(session)

    handler.insert_tsdb([{"a": 1, "b": 2, "c": 9}, {"a": 3, "b": 4}])

    assert session.executed == [
        ("INSERT", [{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
    ]
    assert session.commit_calls == 1
    assert session.rollback_calls == 0


def test_insert_tsdb_bulk_integrity_error_falls_back_to_linear() -> None:
    session = RecordingSession(behaviors=[IntegrityError("", {}, Exception("err"))])
    handler = build_handler(session)

    handler.insert_tsdb([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

    # first bulk attempt failed, then two linear inserts
    assert session.rollback_calls >= 1
    assert session.commit_calls == 2  # commits from linear path
    assert session.executed == [
        ("INSERT", {"a": 1, "b": 2}),
        ("INSERT", {"a": 3, "b": 4}),
    ]


def test_insert_tsdb_retries_after_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    failing_session = RecordingSession(behaviors=[RuntimeError("fail")])
    recovering_session = RecordingSession()
    handler = build_handler(failing_session)

    def reset_session() -> None:
        handler.session = cast(Any, recovering_session)

    monkeypatch.setattr(handler, "_reset_session", reset_session)

    handler.insert_tsdb([{"a": 9, "b": 8}])

    assert failing_session.rollback_calls == 1
    assert recovering_session.executed == [("INSERT", [{"a": 9, "b": 8}])]
    assert recovering_session.commit_calls == 1


def test_insert_tsdb_returns_on_empty() -> None:
    session = RecordingSession()
    handler = build_handler(session)
    handler.insert_tsdb([])
    assert not session.executed


def test_insert_tsdb_linear_accepts_dict() -> None:
    session = RecordingSession()
    handler = build_handler(session)
    handler.insert_tsdb_linear({"a": 1, "b": 2})
    assert session.executed == [("INSERT", {"a": 1, "b": 2})]


def test_insert_tsdb_linear_not_supported_error() -> None:
    class FailingTable(DummyTable):
        def insert(self) -> DummyInsertBuilder:
            class Builder(DummyInsertBuilder):
                def values(self, row: Any) -> tuple[str, Any]:  # pylint: disable=unused-argument
                    raise NotSupportedError("stmt", None, Exception("bad"))

            return Builder()

    session = RecordingSession()
    handler = build_handler(session)
    handler.table = cast(Any, FailingTable())

    handler.insert_tsdb_linear([{"a": 1, "b": 2}])

    assert session.rollback_calls == 1
    assert not session.executed


def test_insert_tsdb_linear_integrity_error() -> None:
    session = RecordingSession(behaviors=[IntegrityError("", {}, Exception("err"))])
    handler = build_handler(session)

    handler.insert_tsdb_linear([{"a": 1, "b": 2}])

    assert session.rollback_calls == 1
    assert session.commit_calls == 0
    assert not session.executed


def test_insert_tsdb_linear_empty_input() -> None:
    session = RecordingSession()
    handler = build_handler(session)
    handler.insert_tsdb_linear([])
    assert not session.executed


def test_insert_tsdb_linear_recovers_from_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession(behaviors=[RuntimeError("boom")])
    handler = build_handler(session)

    def reset_session() -> None:
        session.behaviors = []

    monkeypatch.setattr(handler, "_reset_session", reset_session)

    handler.insert_tsdb_linear([{"a": 1}])  # missing 'b' covers serialization branch

    assert session.rollback_calls == 1
    assert session.commit_calls == 1
    assert session.executed == [("INSERT", {"a": 1})]


def test_insert_tsdb_requires_session() -> None:
    handler = DataHandler("u", "p", "h", "5432", "db")
    handler.table = cast(Any, DummyTable())
    handler.session = None
    handler.column_list = ["a"]

    with pytest.raises(RuntimeError):
        handler.insert_tsdb([{"a": 1}])


def test_insert_tsdb_linear_requires_session() -> None:
    handler = DataHandler("u", "p", "h", "5432", "db")
    handler.table = cast(Any, DummyTable())
    handler.session = None
    handler.column_list = ["a"]

    with pytest.raises(RuntimeError):
        handler.insert_tsdb_linear([{"a": 1}])
