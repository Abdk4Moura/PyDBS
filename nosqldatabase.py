#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import hashlib
import os
from typing import Callable, Iterable
from database import Collection
from functools import cache

VERBOSE_MODE: bool = False
OPERATION_STACK = []
DEFAULT_PROMPT_POSITIVE = ("Yes[Y]", "No[N]")


def record_hash(record):
    record_hash = hashlib.sha256()
    for key, value in record.items():
        record_hash.update(str(key).encode("utf-8"))
        record_hash.update(str(value).encode("utf-8"))

    return record_hash.hexdigest()


def generate_unique_id(record):
    salt = os.urandom(16)
    record_hash = hashlib.sha256()
    record_hash.update(salt)
    record_hash.update(json.dumps(record))

    return record_hash.hexdigest()


def operation_failed() -> None:
    emit(f"[OPERATION_STACK.pop()] failed.")


def operation_succesful() -> None:
    emit(f"[{OPERATION_STACK.pop()}] succesful.")


def operation_start(name_of_operation: str) -> None:
    emit(f"[{name_of_operation}]: running...")
    OPERATION_STACK.append(name_of_operation)


def notify(operation_name: str):
    """A decorator factory for different operation types.
    @operation_name: The name of the operation to be pushed into the operation stack

    Any method or function that utilises could utilise the `confirm` function if
    there's any wish to halt the execution for whatever prompt
    """

    def _notify(func):
        """Decorator to make operations notify when succesful"""

        def _notify_(*args, **kwargs):
            global VERBOSE_MODE
            value_before = len(OPERATION_STACK)
            verbose = kwargs.get("verbose")
            if not verbose and VERBOSE_MODE:
                VERBOSE_MODE = False
            operation_start(operation_name)
            # define @ref dirty1
            # the function to be used therewith has at least a dirty component
            # like `operation_start`, `operation_succesful` and `operation_failed`
            res = func(*args, **kwargs)
            # therefore len(OPERATION_STACK) before calling the function and after
            # could be different
            value_after = len(OPERATION_STACK)
            if value_before != value_after:
                operation_succesful()
            # temporary verbose matters
            if not verbose and VERBOSE_MODE:
                VERBOSE_MODE = not VERBOSE_MODE

            return res

        return _notify_

    return _notify


def emit(
    *messages: object, sep=" ", end="\n", flush=False, operation_info=None
) -> None:
    if operation_info != None:
        OPERATION_STACK.append(operation_info)
    if VERBOSE_MODE:
        print(*messages, sep=sep, end=end, flush=flush)


def confirm(
    *messages, callback: Callable, expected: Iterable, negative_callback: Callable
) -> bool:
    if VERBOSE_MODE:
        response = input(" ".join(messages) + "[Yes[Y], No[N]]")
        if response in expected:
            callback()
            return True
        if negative_callback:
            negative_callback()
    return False


class NoSQLDatabase:
    def __init__(self, database_path):
        self.database_path = database_path

    @notify("{mutate}: CREATE_COLLECTION")
    def create_collection(self, collection_name):
        create_a_new_ = lambda: os.mkdir(self.database_path)
        if os.path.exists(self.database_path):
            response = confirm(
                "Do you want to create  the database path a new path?",
                callback=create_a_new_,
                expected=DEFAULT_PROMPT_POSITIVE,
            )
            if not response:
                operation_failed()
                return
        elif self.collection_exists(collection_name):
            emit("Collection", collection_name, "already exists")
            operation_failed()
            return
        else:
            create_a_new_()
        os.mkdir(os.path.join(self.database_path, collection_name))

        return Collection(collection_name, parent=self)

    @notify("{query} GET COLLECTIONS")
    @cache
    def get_collections(self) -> list[Collection]:
        collection_res = []
        for collection_name in os.listdir(self.database_path):
            collection_res.append(Collection(collection_name, parent=self))
        return collection_res

    @notify("{mutate} DELETE COLLECTION")
    def delete_collection(self, collection_name):
        try:
            os.rmdir(os.path.join(self.database_path, collection_name))
        except Exception:
            operation_failed()

    @notify("{query}: COLLECTION_EXISTS")
    def collection_exists(self, collection_name: str, verbose=False) -> bool:
        """
        Checks if a collection exists in the database.

        Args:
            collection_name: The name of the collection to check.

        Returns:
            True if the collection exists, False otherwise.
        """

        collection_path = os.path.join(self.database_path, collection_name)
        return os.path.exists(collection_path)

    # TODO: insert_record should allow qeueing
    @notify("{mutate}: INSERT_RECORD")
    def insert_record(self, collection_name, record):
        # @ ref dirty1
        # the confirm function is dirty because it fires a
        # dirty function
        if not confirm(
            "create a new collection",
            callback=lambda: self.collection_exists(collection_name),
            expected=DEFAULT_PROMPT_POSITIVE,
        ):
            return

        generat

        with open(
            os.path.join(self.database_path, collection_name, record["id"]), "w"
        ) as f:
            f.write(json.dumps(record))

    @notify("{query}: GET_RECORD")
    def get_records(self, collection_name):
        records = []
        folder_path = os.path.join(self.database_path, collection_name)
        for file in os.listdir(folder_path):
            with open(os.path.join(folder_path, file)) as f:
                record = json.load(f)
                emit("record", record)
                records.append(record)

        return records

    @notify("{mutate}: DELETE_RECORD")
    def delete_record(self, collection_name, record_id):
        os.remove(os.path.join(self.database_path, collection_name, record_id))


if __name__ == "__main__":
    # make VERBOSE_MODE = False for debuggin purposes
    VERBOSE_MODE = True
    # TODO: Will test the default database path later
    database = NoSQLDatabase("path/to/database")

    # Create a collection
    database.create_collection("my_collection1")

    # Insert a record
    record = {"id": "1", "name": "John Doe"}
    database.insert_record("my_collection", record)

    # Get all records
    records = database.get_records("my_collection")

    # Delete a record
    #  database.delete_record("my_collection", "1")
