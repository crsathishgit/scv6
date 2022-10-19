import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *
import time
import tracemalloc

def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True


def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True


def do_validation():
    # read the data
    data = pd.read_csv('gitdata.csv')

    # define validation elements
    decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

    # define validation schema
    schema = pandas_schema.Schema([
            Column('Experiment ', decimal_validation + null_validation),
            Column('Virus ', decimal_validation),
            Column('Cell', decimal_validation),
            Column('MOI', int_validation + null_validation),
            Column('hpi', decimal_validation),
            Column('Titer', decimal_validation + null_validation),
            ])

    # apply validation
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    data_clean = data.drop(index=errors_index_rows)

    # save data
    pd.DataFrame({'col':errors}).to_csv('giterrors.csv')
    data_clean.to_csv('git_clean_data.csv')

if __name__ == '__main__':
    # do_columns_validation()
    print("------------------")
    ts = time.time()
    print(f'Start Time {ts}! ')
    tracemalloc.start()
    do_validation()
    print("Memory usage: ")
    print(tracemalloc.get_traced_memory())
    tracemalloc.stop()
    ts = time.time()
    print(f'End Time {ts}! ')