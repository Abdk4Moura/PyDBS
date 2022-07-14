from os import path, mkdir, rmdir, remove, chdir, getcwd
from typing import Tuple, Callable, Any, Dict, TypedDict
from muddle_ import grs



def rm(f_or_d):
    try:
        remove(f_or_d)
    except OSError:
        rmdir(f_or_d)


## Defining Universal Closure
def withLocation():

    location = collection = table = None
    databases_ = {}
    collections_ = {}

    def setLoca_(string):
        nonlocal location
        location = string

    def setColle_(string):
        nonlocal collection
        collection = string

    def setTable_(string):
        nonlocal table
        table = string

    def getLoca_():
        return location

    def getColle_():
        return collection

    def getTable_():
        return table

    def databases():
        return databases_

    def collections():
        return collections_

    return setLoca_, getLoca_, setColle_, getColle_, setTable_, getTable_, databases, collections


setLocation, getLocation, setCollection, \
getCollection, setTable, getTable, \
databases, colls = withLocation()


# Database definitions
class Table:
    def __init__(self, name, columns, parent=getCollection()):
        if not parent:
            raise TypeError('NoneType is not a collection exists. No collection'
                            'object has been created in the current session')
        elif not colls().get(parent, 0):
            raise TypeError(f'parent {parent} not a collection')

        self.name = name
        self.location = path.join(parent, name)


class Collection:
    # def exposee(self):
    #     file = open(self.name, 'r')
    #     three_lines = []
    #
    #     for i in range(3):
    #         three_lines.append(file.readline())
    #
    #     return three_lines

    def __init__(self, name, parent=getLocation()):
        location = None
        parent_ = parent
        parent = databases().get(parent, None)
        if parent:
            location = parent.location
            location = path.join(location, name)
            self.parent = parent

        else:
            raise TypeError(f'`{parent_}` not a database')

        self.name = name
        self.location = location
        self.tables = set()
        colls()[location] = self

    # def createTable(self, name, datatype):
    #     if not self.collections:
    #         mkdir('default')
    #
    #     setLocation(self.location)
    #     table_location = path.join(location, name)
    #     file = open(table_location, 'w+')
    #     file.write('datatype = ' + datatype)
    #     file.close()
    #
    #     return self.Table(table_location, datatype)

    def showTables(self):
        for i in self.tables:
            print()


class Database:
    def __init__(self, location=path.join(getcwd(),'DB'+grs()), collections=set()):
        self.location = location

        if not collections:
            collections.add('Default')

        self.collections = collections
        mkdir(location)

        for collection in collections:
            mkdir(path.join(location, collection))

        collection = collections['Default']
        databases()[location] = self
