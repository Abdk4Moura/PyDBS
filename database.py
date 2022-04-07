from os import path
from os_tools import mk, mkdir, rm

location = None
collection = None
table = None


class Database:
    def __init__(self, location, collections=[]):
        global collection, table

        self.location = location

        if not collections:
            collections.add('Default')

        self.collections = collections
        mkdir(location)

        for collection in collections:
            mkdir(path.join(location, collection))

        collection = collections['Default']

class Collection:
    def __init__(self, name):
        self.name = name
        self.location = path.join(location, name)
        self.tables = set({})

    def __del__(self):
        rm(path.join(location, self.name))

    def createTable(self, name, datatype):
        if not self.collections:
            mkdir('default')
        location = self.location
        table_location = path.join(location, name)
        file = open(table_location, 'w+')
        file.write('datatype = ' + datatype)
        file.close()

        return Table(table_location, datatype)

    def showTables(self):
        for i in self.tables:
            print()

class Table:
    def __init__(self, name, columns):
        self.name = name

    def exposee(self):
        file = open(self.name, 'r')
        three_lines = []

        for i in range(3):
            three_lines.append(file.readline())

        return three_lines
