import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *
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

def checkNone(n):
    if(np.isnan(n)):
        return False
    return True


def getSchema():
    # define validation elements
    decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'decimal_validation: is not decimal')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'int_validation: is not integer')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'null_validation: this field cannot be null')]
    none_validation = [CustomElementValidation(lambda n:  checkNone(n), 'checkNone: this field cannot be null')]
    # extra_column_validation = [CustomElementValidation(lambda e:  extraColumn(e), 'this field cannot be null')]

    # define validation schema
    schema = pandas_schema.Schema([
            Column('Experiment ', null_validation),
            Column('Virus ', null_validation),
            Column('Cell', null_validation),
            Column('MOI', int_validation  + none_validation),
            Column('hpi', decimal_validation),
            Column('Titer', decimal_validation + none_validation),
            ])
    return schema
