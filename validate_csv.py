import sys
import csv
from operator import truediv
import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
import numpy as np
from decimal import *
import time
import tracemalloc
import weather_csv_schema

delimiter = ";"

args = sys.argv

print(args[1])

def DoesItContainNoneAsValue(keys, line):
  for key in keys:
      if(line[key] == None):
        return True
  return False

def constructHeader(keys):
    header = ''
    lastElement = keys[-1]
    for key in keys:
      if(key == None):
        header += 'None'
      else:
        if(key == lastElement):
          header += key 
        else:
          header += key + delimiter
    return header

def constructRow(row):
  keys = list(row.keys())
  lastElement = keys[-1]
  strRow = ''
  for key in keys:
    if(key == lastElement):
      strRow += str(row[key])
    else:
      strRow += str(row[key]) + delimiter
  return strRow

def writeRowsIntoCSV(rows, filename):
    writtenHeader = False
    with open(filename, 'w') as the_file:
      for row in rows:
        rowKeys = list(row.keys())
        print('rowKeys--->')
        print(rowKeys)
        header = constructHeader(rowKeys)
        if(writtenHeader == False):
          writtenHeader = True
          the_file.write(header + "\n")
        the_file.write(constructRow(row) + "\n")

def preprocess_segregate_error_rows(columns, columnnames):
    print("preprocess_segregate_error_rows....")
    with open('./csvsource/weatherSubsetPipe.csv', 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=delimiter, fieldnames=columnnames)
        clean_rows = []
        problem_rows = []
        for line in csv_reader:
            try:
              keys = list(line.keys())
              print("keys----------->")
              print(keys)
              print("<-----------keys")
              numberOfColumns = len(keys)
              print(line[keys[0]])
              if(numberOfColumns == columns):
                result = DoesItContainNoneAsValue(keys, line)
                if(result == True):
                  problem_rows.append(line)
                else:
                  clean_rows.append(line)
              else:
                problem_rows.append(line)
              print(numberOfColumns)
            except: 
              clean_rows.append(line)

        print("# of clean_rows") 
        print(clean_rows) 
        print("----------------------------------------------------")


        writeRowsIntoCSV(clean_rows, './csvproduced/clean_rows_preprocessed.csv')
        print("----------------------------------------------------")
        print("# of problem_rows")   
        print(len(problem_rows))
        print(problem_rows)
        writeRowsIntoCSV(problem_rows, './csvproduced/problem_rows_preprocessed.csv')


def do_validation():
    print("===================>>>>>>")
    preprocess_segregate_error_rows(weather_csv_schema.getColumnsCount(), weather_csv_schema.getColumnNames())
    print("<<<<<<===================")
    # read the data
    data = pd.read_csv('./csvproduced/clean_rows_preprocessed.csv', sep=delimiter  , engine='python')

    # define validation schema
    schema = weather_csv_schema.getSchema()

    # apply validation
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    data_clean = data.drop(index=errors_index_rows)

    # save data
    pd.DataFrame({'col':errors}).to_csv('./csvproduced/git_errors.csv', sep=delimiter, index=False)
    print("Number of clean rows:")
    print(len(data_clean))

    print("Number of dirty rows:")
    print(len(errors_index_rows))
    data_clean.to_csv('./csvproduced/git_clean_data.csv', sep=delimiter, index=False)

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
