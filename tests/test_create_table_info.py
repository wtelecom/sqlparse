# -*- coding: utf-8 -*-

import re
from unittest import TestCase

from sqlparse.filters import get_create_table_info
from sqlparse.lexer import tokenize


# TODO: Use Py.Test for testing compat from 2.6, 2.7, 3.3-3.5+
# TODO: Format test
class TestCasePy27Features(object):
    class __AssertRaisesContext(object):
        def __init__(self, expected_exception, expected_regexp):
            self.expected = expected_exception
            self.expected_regexp = expected_regexp

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb):
            if exc_type is None:
                raise self.failureException('%s not raised' % exc_type.__name__)
            if not issubclass(exc_type, self.expected):
                return False
            self.exception = exc_value
            expected_regexp = self.expected_regexp
            if isinstance(expected_regexp, basestring):
                expected_regexp = re.compile(expected_regexp)
            if not expected_regexp.search(str(exc_value)):
                raise self.failureException('"%s" does not match "%s"' %
                                            (expected_regexp.pattern, str(exc_value)))
            return True

    """Adds simple replacements for unittest features not available before Python 2.7"""
    def assertRaisesRegexp(self, expected_exception, expected_regexp):
        return self.__AssertRaisesContext(expected_exception, expected_regexp)


# TODO: Update exception in test for Py3 compat
class Test_GetCreateTableInfo(TestCase, TestCasePy27Features):
    sql1 = """
        CREATE TABLE item (
            id INT PRIMARY KEY NOT NULL,
            type VARCHAR(3) NOT NULL,
            score DOUBLE,
            url TEXT NULL,
            text TEXT NOT NULL,
            item2other INT NULL,

            FOREIGN KEY(item2other) REFERENCES othertable(id)
        );
    """

    sql2 = """
        CREATE UNIQUE INDEX t1b ON t1(b);
    """

    sql3 = """
        CREATE TABLE a ( afield INT PRIMARY KEY NOT NULL );
        CREATE TABLE b ( bfield VARCHAR(10) NULL ) ;
        CREATE TABLE c ( cfield TEXT PRIMARY KEY NOT NULL ) this gets ignored;
        CREATE TABLE d ( dfield NVARCHAR PRIMARY KEY )
    """

    sql4 = """
        CREATE TABLE example (
            id INT,
            data VARCHAR(10)
        ) TYPE=innodb;
    """

    sql5 = """
        CREATE TABLE mydb.mytable (
            `a` INT(10) unsigned not null default '0',
            `b` DECIMAL(6,0) unsigned NOT null ,
            `c` DATE,
            `d` DECIMAL(4,0) unsigned not null ,
            `e` VARCHAR(6) not null ,
            `f` VARCHAR(1) not null ,
            `g` VARCHAR(1) not null,
            `h` DECIMAL(13,2)unsigned not null ,
            `i` DECIMAL(4,0)unsigned ,
            `j` VARCHAR(30)unsigned not null,
            `k` DECIMAL(13,2) unsigned not null,
            `l` VARCHAR(1)not null ,
            `m` DECIMAL(13,2) unsigned not null,
            `n` VARCHAR(1) unsigned not null,
            PRIMARY KEY (`a`)
        )ENGINE=InnoDB;
    """

    sql6 = 'SELECT * FROM a'
    sql7 = 'CREATE TABLE ('
    sql8 = 'CREATE TABLE'
    sql9 = 'CREATE TABLE t (,)'
    sql10 = 'CREATE TABLE t ( a NULL )'

    sql11 = """
        CREATE TABLE t (
            a INT,
            PRIMARY KEY (a),
            FOREIGN KEY (a) REFERENCES other(id)
        )
    """

    sql12 = 'CREATE TABLE a,'
    sql13 = 'CREATE TABLE t (a INT, a INT)'
    sql14 = 'CREATE TABLE pair ( id VARCHAR(10) PRIMARY KEY NOT NULL, source VARCHAR(3) NOT NULL, target VARCHAR(3) NOT NULL );'
    sql15 = """
        CREATE TABLE t (
            a TEXT NOT NULL,
            b TEXT NOT NULL,
            PRIMARY KEY (a),
            FOREIGN KEY (a) REFERENCES other(id),
            FOREIGN KEY (b) REFERENCES other(id)
        )
    """

    def test_get_create_table_info1(self):
        info = get_create_table_info(tokenize(self.sql1))

        self.assertEqual(info, [('item', {
            0: ('id',         'INT',     'NOT NULL'),
            1: ('type',       'VARCHAR', 'NOT NULL'),
            2: ('score',      'DOUBLE',  None),
            3: ('url',        'TEXT',    'NULL'),
            4: ('text',       'TEXT',    'NOT NULL'),
            5: ('item2other', 'INT',     'NULL'),
        })])

    def test_get_create_table_info3(self):
        info = get_create_table_info(tokenize(self.sql3))

        self.assertEqual(info, [
            ('a', {
                0: ('afield', 'INT', 'NOT NULL'),
            }),
            ('b', {
                0: ('bfield', 'VARCHAR', 'NULL'),
            }),
            ('c', {
                0: ('cfield', 'TEXT', 'NOT NULL'),
            }),
            ('d', {
                0: ('dfield', 'NVARCHAR', None),
            }),
        ])

    def test_get_create_table_info4(self):
        info = get_create_table_info(tokenize(self.sql4))

        self.assertEqual(info, [('example', {
            0: ('id', 'INT', None),
            1: ('data', 'VARCHAR', None),
        })])

    def test_get_create_table_info5(self):
        info = get_create_table_info(tokenize(self.sql5))

        self.assertEqual(info, [('mydb.mytable', {
             0: ('a', 'INT',     'NOT NULL'),
             1: ('b', 'DECIMAL', 'NOT NULL'),
             2: ('c', 'DATE',    None),
             3: ('d', 'DECIMAL', 'NOT NULL'),
             4: ('e', 'VARCHAR', 'NOT NULL'),
             5: ('f', 'VARCHAR', 'NOT NULL'),
             6: ('g', 'VARCHAR', 'NOT NULL'),
             7: ('h', 'DECIMAL', 'NOT NULL'),
             8: ('i', 'DECIMAL', None),
             9: ('j', 'VARCHAR', 'NOT NULL'),
            10: ('k', 'DECIMAL', 'NOT NULL'),
            11: ('l', 'VARCHAR', 'NOT NULL'),
            12: ('m', 'DECIMAL', 'NOT NULL'),
            13: ('n', 'VARCHAR', 'NOT NULL'),
        })])

    def test_get_create_table_info11(self):
        info = get_create_table_info(tokenize(self.sql11))

        self.assertEqual(info, [('t', {
            0: ('a', 'INT', None),
        })])

    def test_get_create_table_info14(self):
        info = get_create_table_info(tokenize(self.sql14))

        self.assertEqual(info, [('pair', {
            0: ('id', 'VARCHAR', 'NOT NULL'),
            1: ('source', 'VARCHAR', 'NOT NULL'),
            2: ('target', 'VARCHAR', 'NOT NULL'),
        })])

    def test_get_create_table_info15(self):
        info = get_create_table_info(tokenize(self.sql15))

        self.assertEqual(info, [('t', {
            0: ('a', 'TEXT', 'NOT NULL'),
            1: ('b', 'TEXT', 'NOT NULL'),
        })])

    def test_get_create_table_info_errors(self):
        for test, expected_regexp in (
            ('sql6', 'Not a CREATE statement'),
            ('sql2', 'Not a CREATE TABLE statement'),
            ('sql7', 'No table name given'),
            ('sql8', 'Unexpected end state'),
            ('sql9', 'No column name given'),
            ('sql10', 'No column type given'),
            ('sql12', 'No opening paren for CREATE TABLE'),
            ('sql13', 'Duplicate column name'),
        ):
            try:
                with self.assertRaisesRegexp(ValueError, expected_regexp):
                    get_create_table_info(tokenize(getattr(self, test)))
            except self.failureException, e:
                raise self.failureException('%s (in test %r)' % (e, test))


