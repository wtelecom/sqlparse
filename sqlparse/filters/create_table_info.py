# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Andi Albrecht, albrecht.andi@gmail.com
#
# This module is part of python-sqlparse and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php

from sqlparse import tokens as T
from types import GeneratorType


# FIXME: Don't use Pipeline if not necessary. Replace with stream
class Pipeline(list):
    """Pipeline to process filters sequentially"""

    def __call__(self, stream):
        """Run the pipeline

        Return a static (non generator) version of the result
        """

        # Run the stream over all the filters on the pipeline
        for filter in self:
            # Functions and callable objects (objects with '__call__' method)
            if callable(filter):
                stream = filter(stream)

            # Normal filters (objects with 'process' method)
            else:
                stream = filter.process(None, stream)

        # If last filter return a generator, staticalize it inside a list
        if isinstance(stream, GeneratorType):
            return list(stream)
        return stream


def get_create_table_info(stream):
    """
    Function that returns the columns of a CREATE TABLE statement including
    their type and NULL declaration.

    The nullable declaration is None if not specified, else NOT NULL or NULL.
    >>> import lexer as lex
    >>> get_create_table_info(lex.tokenize('CREATE TABLE t (a INT NOT NULL )'))
    [('t', {0: ('a', 'INT', 'NOT NULL')})]
    """
    pipe = Pipeline()
    pipe.append(InfoCreateTable())
    return pipe(stream)


# FIXME: Use StripWhitespace Filter instead of removed StripWhitespace
def StripWhitespace(stream):
    "Strip the useless whitespaces from a stream leaving only the minimal ones"
    last_type = None
    has_space = False
    ignore_group = frozenset((T.Comparison, T.Punctuation))

    for token_type, value in stream:
        # We got a previous token (not empty first ones)
        if last_type:
            if token_type in T.Whitespace:
                has_space = True
                continue

        # Ignore first empty spaces and dot-commas
        elif token_type in (T.Whitespace, T.Whitespace.Newline, ignore_group):
            continue

        # Yield a whitespace if it can't be ignored
        if has_space:
            if not ignore_group.intersection((last_type, token_type)):
                yield T.Whitespace, ' '
            has_space = False

        # Yield the token and set its type for checking with the next one
        yield token_type, value
        last_type = token_type


# FIXME: Refactor code into smaller functions
class InfoCreateTable(object):
    # sqlparse outputs some tokens as Keyword at places where they are names
    ALLOWED_KEYWORD_AS_NAME = 'data', 'source', 'type'

    def process(self, stack, stream):
        class St(object):
            create = 0
            table = 1
            table_name = 2
            create_table_open_paren = 3
            column_name = 4
            column_type = 5
            column_ignore_rest = 6
            ignore_rest = 7
            finished = 8
            ignore_remaining_statement = 9

        state = St.create
        error = ''
        parens = 0

        for token_type, value in StripWhitespace(stream):

            # Ignore comments
            if token_type in (T.Comment, T.Whitespace):
                continue

            if state == St.create:
                table_name = ''
                columns = {}  # index => (name, type)
                column_names = set()
                column = None

                if token_type in T.Keyword and value.upper() == 'CREATE':
                    state = St.table
                else:
                    error = 'Not a CREATE statement'
            elif state == St.table:
                if token_type in T.Keyword and value.upper() == 'TABLE':
                    state = St.table_name
                else:
                    error = 'Not a CREATE TABLE statement'
            elif state == St.table_name:
                if token_type in T.Name:
                    state = St.create_table_open_paren
                    table_name += value
                else:
                    error = 'No table name given'
            elif state == St.create_table_open_paren:
                if token_type in T.Punctuation:
                    if value == '(':
                        state = St.column_name
                    elif value == '.':
                        table_name += '.'
                        state = St.table_name

                if state == St.create_table_open_paren:
                    error = 'No opening paren for CREATE TABLE'
            elif state == St.column_name:
                if (token_type in T.Name or
                        (token_type in T.Keyword and value.lower() in
                            InfoCreateTable.ALLOWED_KEYWORD_AS_NAME)):
                    column = [self._to_column_name(value), None, None]
                    state = St.column_type
                elif token_type in T.Keyword:
                    state = St.ignore_rest
                else:
                    error = 'No column name given'
            elif state == St.column_type:
                if token_type in T.Name:
                    column[1] = value
                    state = St.column_ignore_rest
                else:
                    error = 'No column type given'
            elif state == St.ignore_rest:
                if token_type in T.Punctuation:
                    if value == '(':
                        parens += 1
                    elif value == ')':
                        if parens == 0:  # closes 'CREATE TABLE ('
                            state = St.finished
                        else:
                            parens -= 1
                    elif value == ',' and parens == 0:
                        state = St.column_name
            elif state == St.column_ignore_rest:
                # ignore anything in parens
                if token_type in T.Punctuation and parens == 0:
                    add_column = False

                    if value == '(':
                        parens += 1
                    elif value == ')':
                        # closes 'CREATE TABLE ('
                        if parens == 0:
                            state = St.finished

                            add_column = True
                        else:
                            error = 'Logic error (end of column declaration#1)'
                    elif value == ',':
                        add_column = True

                        state = St.column_name

                    if add_column:
                        if column[0] in column_names:
                            error = 'Duplicate column name: %s' % column[0]
                        else:
                            column_names.add(column[0])
                            # Store column index, name and type
                            columns[len(columns)] = tuple(column)

                        column = None
                elif token_type in T.Keyword and parens == 0:
                    keyword_value = value.upper()
                    if keyword_value in ('NULL', 'NOT NULL'):
                        column[2] = keyword_value
                elif token_type in T.Punctuation and parens > 0:
                    # ignore anything in parens
                    if value == '(':
                        parens += 1
                    elif value == ')':
                        parens -= 1

            # else ignore until comma or end of statement
            elif state == St.finished:
                # Finished one CREATE TABLE statement,
                # yield result and try to parse next statement
                # (after semicolon)
                yield (table_name, columns)

                if token_type in T.Punctuation and value == ';':
                    state = St.create
                else:
                    state = St.ignore_remaining_statement
            elif state == St.ignore_remaining_statement:
                # Ignore until semicolon
                if token_type in T.Punctuation and value == ';':
                    state = St.create
            else:
                error = 'Unknown state %r' % state

            if error:
                raise ValueError(
                    '%s (token_type: %r, value: %r, column: %r)' % (
                        error, token_type, value, column))

        if not error:
            # no more tokens after ')'
            if state == St.finished:
                yield (table_name, columns)

            if state not in (
                St.create, St.finished, St.ignore_remaining_statement):
                error = ('Unexpected end state %r (token_type: '
                         '%r, value: %r, column: %r)' % (
                             state, token_type, value, column))

        if error:
            raise ValueError(error)

    @staticmethod
    def _to_column_name(token_value):
        return token_value.strip(u'`´')
