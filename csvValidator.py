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
import schemaBuilder
import json 
import glob 

args = sys.argv
jsonFile = args[1]
delimiter = ","
columnsFromJSON = []
sourceCSVfileName = ""
sourceCSVsFolder=""
sourceCSVfileNameNoExt = ""
errorFolderPath=""
fileHeaderRequired = True
entirebatchRejection = False

def getColumnsFromJSON():
  global delimiter
  global columnsFromJSON
  global sourceCSVfileName 
  global sourceCSVsFolder
  global sourceCSVfileNameNoExt
  global fileHeaderRequired
  global entirebatchRejection
  global errorFolderPath
  f = open(jsonFile)
  data = json.load(f)
  delimiter = str(data["fileSeparator"])
  # sourceCSVfileName = str(data["fileName"])
  sourceCSVsFolder = str(data["folderpath"])
  errorFolderPath = str(data["errorFolderPath"])
  # sourceCSVfileNameNoExt = sourceCSVfileName.split(".")[1].split("/")[2]
  fileHeaderRequired = data["fileHeaderRequired"] 
  entirebatchRejection = data["entirebatchRejection"]
  for item in data["fileHeaders"]:
    columnsFromJSON.append(item)
  print(columnsFromJSON)
  # print("sourceCSVfileNameNoExt--------------->")
  # print(sourceCSVfileNameNoExt)
  # print(fileHeaderRequired)
  return columnsFromJSON

def listAllSourceCSVFiles():
    global sourceCSVfileName 
    global sourceCSVfileNameNoExt 
    files = glob.glob(f'{sourceCSVsFolder}/*.csv')
    print("(((files)))")
    print(files)
    sourceCSVfileName = files[0]
    print("sourceCSVfileName------------->")
    print(sourceCSVfileName)
    print("<-------------sourceCSVfileName")
    sourceCSVfileNameNoExt = sourceCSVfileName.split("/")[-1].split(".")[0]
    return files

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
        header = constructHeader(rowKeys)
        if(writtenHeader == False):
          writtenHeader = True
          the_file.write(header + "\n")
        the_file.write(constructRow(row) + "\n") 

def reportColumnNamesError(keys, columnnames):
    print("reportColumnNamesError --->")
    keyslen = len(keys)
    columnnameslen = len(columnnames)
    columnErrors = []
    if(keyslen == columnnameslen) :
      for i in range(keyslen):
        if(keys[i] != columnnames[i]):
          err = "CSV file contains \"" + keys[i] + "\" as column name.    But schema contains \"" + columnnames[i] + "\""
          columnErrors.append(err)
    elif (keyslen > columnnameslen):
      for i in range(keyslen):
        try:
          index = columnnames.index(keys[i])
        except:
          print(f'The index of {keys[i]} not found')
          err = f'CSV file contains the column "{keys[i]}". But the schema doesn\'t contain.'
          columnErrors.append(err)
    elif (keyslen < columnnameslen):
      for i in range(columnnameslen):
        try:
          index = keys.index(columnnames[i])
        except:
          err = f'Schema file contains the column "{columnnames[i]}". But the CSV doesn\'t contain.'
          columnErrors.append(err)
    print(columnErrors)
    with open(sourceCSVsFolder + "/columnErrors.txt", 'w') as the_file:
        for err in columnErrors:
           the_file.write(err + "\n")

def scrutinizeCleanLine(line, columnnames):
  for column in columnnames:
    line[column] = line[column].replace(delimiter, "~~~")
  return line

def preprocess_segregate_error_rows(columns, columnnames):
    print("preprocess_segregate_error_rows....")
    print(columnnames)
    with open(sourceCSVfileName, 'r') as csv_file: 
        if(fileHeaderRequired == True):
          csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
        else:
          csv_reader = csv.DictReader(csv_file, delimiter=delimiter, fieldnames=columnnames)
        # get the columns from actual csv file and compare with JSON header
        # if not matching raise error and halt.
        clean_rows = []
        problem_rows = []
        columnCheckDone = False
        for line in csv_reader:
            try:
              keys = list(line.keys())
              if(columnCheckDone == False):
                columnCheckDone = True
                if(keys != columnnames): #Things are OK
                    reportColumnNamesError(keys, columnnames)
                    sys.exit()
              numberOfColumns = len(keys)
              if(numberOfColumns == columns):
                result = DoesItContainNoneAsValue(keys, line)
                if(result == True):
                  problem_rows.append(line)
                else:
                  if(entirebatchRejection == True):
                      print("entirebatchRejection.......1")
                      sys.exit()
                  clean_rows.append(scrutinizeCleanLine(line, columnnames))
              else:
                problem_rows.append(line)
            except: 
              clean_rows.append(scrutinizeCleanLine(line, columnnames))
        print("sourceCSVfileNameNoExt  =========>")
        print(sourceCSVfileNameNoExt)
        print(errorFolderPath)
        
        writeRowsIntoCSV(clean_rows, '.rpt/tlog/.preprocessed/load_date=2022-10-17' + f'/clean_rows_{sourceCSVfileNameNoExt}_preprocessed.csv')
        writeRowsIntoCSV(problem_rows, errorFolderPath + "/" + f'problem_rows_{sourceCSVfileNameNoExt}_preprocessed.csv')


def do_validation(columnsFromJSON):
    print(listAllSourceCSVFiles())
    schema = schemaBuilder.getSchema(columnsFromJSON)
    print("columnsFromJSON ====================>")
    print(columnsFromJSON)
    print("===================>>>>>>")
    preprocess_segregate_error_rows(schemaBuilder.getColumnsCount(), schemaBuilder.getColumnNames())
    print("<<<<<<===================")
    # # read the data
    try:
      data = pd.read_csv('.rpt/tlog/.preprocessed/load_date=2022-10-17' + f'/clean_rows_{sourceCSVfileNameNoExt}_preprocessed.csv', sep=delimiter  , engine='python')
    except  BaseException as e:
      print(f'CSV error........................{str(e)}')
      sys.exit()
    # # define validation schema
  

    # # apply validation
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    print("len(errors_index_rows)----->")
    print(len(errors_index_rows))
    if(len(errors_index_rows) > 0 and entirebatchRejection == True):
      print("entirebatchRejection......2")
      # # save error data
      if(len(errors_index_rows) > 0):
        pd.DataFrame({'col':errors}).to_csv(errorFolderPath + "/" +f'{sourceCSVfileNameNoExt}_errors.csv', sep=delimiter, index=False)
      sys.exit()
    # if entirebatchRejection is true and if errors_index_rows length +ve then raise error and stop
    if(len(errors_index_rows) > 0):
      pd.DataFrame({'col':errors}).to_csv(errorFolderPath + "/" +f'{sourceCSVfileNameNoExt}_errors.csv', sep=delimiter, index=False)
    data_clean = data.drop(index=errors_index_rows)

    print("Number of clean rows:")
    print(len(data_clean))

    print("Number of dirty rows:")
    print(len(errors_index_rows))
    # data_clean.to_csv(f'./csvproduced/{sourceCSVfileNameNoExt}_clean_data.csv', sep=delimiter, index=False)

if __name__ == '__main__':
    # do_columns_validation()
    print("------------------")
    ts = time.time()
    print(f'Start Time {ts}! ')
    tracemalloc.start()
    do_validation(getColumnsFromJSON())
    print("delimiter--->")
    print(delimiter)
    print("Memory usage: ")
    print(tracemalloc.get_traced_memory())
    tracemalloc.stop()
    ts = time.time()
    print(f'End Time {ts}! ')
