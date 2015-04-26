# -*- coding: utf-8 -*-

from types import GeneratorType

from sqlparse.filters_tmp import InfoCreateTable


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
