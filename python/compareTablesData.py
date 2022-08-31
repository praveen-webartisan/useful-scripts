'''
Install Dependencies:
    - pip install mysql-connector-python pandas XlsxWriter
'''

import mysql.connector, logging, traceback, argparse, configparser, os
from typing import Final
import pandas

DB_CONFIG_FILE_PATH = 'db.config'
resultExcelFileName: Final = 'Missing Data.xlsx'

LOGGER = None
DB_CONN = None
tables = None
columnsToCompare = None

# initLogger
def initLogger():
    global LOGGER

    logging.basicConfig(format = '[%(asctime)s] (%(levelname)s): %(message)s', datefmt = '%d-%m-%Y %H:%M:%S')
    LOGGER = logging.getLogger(__file__)
    LOGGER.setLevel(logging.DEBUG)
# initLogger

# getCmdArgs
def getCmdArgs():
    argumentParser = argparse.ArgumentParser()

    argumentParser.add_argument('-table1', help = 'Table - 1: Should be db.table format')
    argumentParser.add_argument('-table2', help = 'Table - 2: Should be db.table format')

    argumentParser.add_argument('-cols', help = 'Columns to compare - Comma separated values')

    argumentParser.add_argument('-config', help = 'Path to Config file: Should follows format: https://docs.python.org/3/library/configparser.html#quick-start')

    return argumentParser.parse_args()
# getCmdArgs

# initDBConn
def initDBConn():
    global tables, DB_CONN, DB_CONFIG_FILE_PATH, LOGGER

    dbConfigSuccess = True
    defaultDBConfig: Final = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '',
    }
    dbConfigFileText = ''

    # Get DB credentials from config file
    if os.path.isfile(DB_CONFIG_FILE_PATH):
        # Read DB config file
        with open(f'{DB_CONFIG_FILE_PATH}', 'r') as fpDBConfig:
            dbConfigFileText = fpDBConfig.read().strip()
        # Read DB config file
    # Get DB credentials from config file

    config = configparser.ConfigParser(allow_no_value = True)

    if dbConfigFileText:
        config.read_string(dbConfigFileText)

    i = 1

    for table in tables:
        dbName = table.get('db')
        DB_CONFIG = None

        if not DB_CONN:
            DB_CONN = {}

        if not(dbName in DB_CONN):
            if config.has_section(f'DB{i}') and config.has_option(f'DB{i}', 'DB_USERNAME') and config.has_option(f'DB{i}', 'DB_PASSWORD'):
                DB_CONFIG = {
                    'host': config.get(f'DB{i}', 'DB_HOST') if config.has_option(f'DB{i}', 'DB_HOST') else defaultDBConfig.get('host'),
                    'port': config.get(f'DB{i}', 'DB_PORT') if config.has_option(f'DB{i}', 'DB_PORT') else defaultDBConfig.get('port'),
                    'database': dbName,
                    'user': config.get(f'DB{i}', 'DB_USERNAME'),
                    'password': config.get(f'DB{i}', 'DB_PASSWORD'),
                }

            if DB_CONFIG:
                DB_CONN[dbName] = mysql.connector.connect(
                    host = DB_CONFIG.get('host'),
                    port = DB_CONFIG.get('port'),
                    database = dbName,
                    user = DB_CONFIG.get('user'),
                    password = DB_CONFIG.get('password'),
                )

        if not DB_CONFIG:
            LOGGER.error(f'DB{i} credentials not available! Please create a config file and pass the path to file in -config argument, Make sure the config file follows format: https://docs.python.org/3/library/configparser.html#quick-start')
            dbConfigSuccess = False
        
        i += 1

    if not dbConfigSuccess:
        quit()
# initDBConn

# fetchTableColumns
def fetchTableColumns(table):
    global DB_CONN

    database = table.get('db')
    tableName = table.get('table')

    columnsQuery = 'SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = %(database)s AND TABLE_NAME = %(table)s ORDER BY ORDINAL_POSITION'

    dbCursor = DB_CONN[database].cursor(dictionary = True)
    dbCursor.execute(columnsQuery, { 'database': database, 'table': tableName })
    rawColumns = dbCursor.fetchall()
    columns = []

    if rawColumns and len(rawColumns) > 0:
        columns = [ tempCol.get('COLUMN_NAME') for tempCol in rawColumns ]

    return columns
# fetchTableColumns

# compareTableDefs
def compareTableDefs():
    global tables, LOGGER, columnsToCompare

    LOGGER.info('Fetching Columns...')

    for table in tables:
        table['columns'] = fetchTableColumns(table)

    LOGGER.info('Comparing Columns...')

    i = 0

    # Iterate tables
    for table1 in tables:
        j = 0
        table1Name = table1.get('table')

        for table2 in tables:
            # Skip same table
            if i != j:
                for column in table1.get('columns'):
                    if not(column in table2.get('columns')):
                        table2['missingColumns'] = table2.get('missingColumns') if 'missingColumns' in table2 else {}

                        if table1Name in table2.get('missingColumns'):
                            table2.get('missingColumns')[ table1Name ].append(column)
                        else:
                            table2.get('missingColumns')[ table1Name ] = [ column ]

            j += 1

        i += 1
    # Iterate tables

    allColumnsMatched = True

    # Iterate tables
    i = 0

    for table in tables:
        tableName = table.get('table')

        if 'missingColumns' in table:
            allColumnsMatched = False

            for table2, missingCols in table.get('missingColumns').items():
                LOGGER.error(f'{tableName} is missing following columns from ' + table2 + ':\n' + (', '.join(missingCols)))

        i += 1
    # Iterate tables

    if allColumnsMatched and columnsToCompare:
        missingCompareCols = []

        for tempCol in columnsToCompare:
            for table in tables:
                if not(tempCol in table.get('columns')):
                    missingCompareCols.append(tempCol)

                # break - need to check only one table
                break

        if len(missingCompareCols) > 0:
            allColumnsMatched = False

            LOGGER.error('Invalid Columns to use for comparison: ' + (', '.join(missingCompareCols)))

    return allColumnsMatched
# compareTableDefs

# fetchTableData
def fetchTableData(table):
    global DB_CONN, LOGGER, columnsToCompare

    database = table.get('db')
    tableName = table.get('table')
    tableColumns = table.get('columns')
    tempColToCompare = columnsToCompare if columnsToCompare else tableColumns

    query = f'SELECT * FROM {database}.{tableName}'

    dbCursor = DB_CONN[database].cursor(dictionary = True)
    dbCursor.execute(query)
    rawData = []
    dataToCompare = []
    i = 0

    while True:
        qResult = dbCursor.fetchmany(10000)

        if qResult:
            i += 1

            LOGGER.info(f'Set {i} fetched. Formatting Set {i}...')

            for tempRow in qResult:
                rowStr = ''

                for tempCol in tempColToCompare:
                    rowStr += '_____' + str(tempRow.get(tempCol))

                dataToCompare.append(rowStr)

            rawData.extend(qResult)
        else:
            LOGGER.info(f'All records are fetched.')
            break

    return (rawData, dataToCompare)
# fetchTableData

# compareTableData
def compareTableData():
    global tables, LOGGER, columnsToCompare

    for table in tables:
        dbName = table.get('db')
        tableName = table.get('table')

        LOGGER.info(f'Fetching {dbName}.{tableName} data...')

        (rawData, dataToCompare) = fetchTableData(table)
        table['rawData'] = rawData
        table['dataToCompare'] = dataToCompare

    LOGGER.info('Comparing Data...')

    # Iterate tables
    i = 0

    for table1 in tables:
        table1DB = table1.get('db')
        table1Name = table1.get('table')
        table1DataIndex = 0

        # Iterate table1 data
        for table1Data in table1.get('dataToCompare'):
            table1RawDat = table1.get('rawData')[table1DataIndex]

            # Iterate other tables
            j = 0

            for table2 in tables:
                # Avoid comparing same table
                if i != j:
                    if not(table1Data in table2.get('dataToCompare')):
                        table2['missingRows'] = table2.get('missingRows') if 'missingRows' in table2 else {}

                        if f'{table1DB}.{table1Name}' in table2.get('missingRows'):
                            table2.get('missingRows')[ f'{table1DB}.{table1Name}' ].append(table1RawDat)
                        else:
                            table2.get('missingRows')[ f'{table1DB}.{table1Name}' ] = [ table1RawDat ]
                # Avoid comparing same table

                j += 1
            # Iterate other tables

            table1DataIndex += 1
        # Iterate table1 data

        i += 1
    # Iterate tables

    allRowsIdentical = True
    sheets = 0
    excelWriter = False

    for table1 in tables:
        database1 = table1.get('db')
        table1Name = table1.get('table')

        if 'missingRows' in table1:
            allRowsIdentical = False

            for table2, missingRows in table1.get('missingRows').items():
                table2Obj = None

                for tempTable in tables:
                    if tempTable.get('db') + '.' + tempTable.get('table') == table2:
                        table2Obj = tempTable
                        break

                if len(table2Obj.get('dataToCompare')) == len(missingRows):
                    LOGGER.error(f'None of the Data is present in {table2} that matches {database1}.{table1Name}!')
                else:
                    if not excelWriter:
                        excelWriter = pandas.ExcelWriter(resultExcelFileName)

                    sheets += 1

                    LOGGER.error(f'{database1}.{table1Name} is missing ' + str(len(missingRows)) + ' rows from ' + table2 + f', Missing data can be found in the {resultExcelFileName} at Sheet {sheets}')

                    df = pandas.DataFrame(missingRows, columns = table1.get('columns'))

                    df.to_excel(excelWriter, index = False)

    if excelWriter:
        excelWriter.save()
    elif os.path.exists(resultExcelFileName):
        os.remove(resultExcelFileName)

    if allRowsIdentical:
        LOGGER.info('Hooray! All the Data are same in all Tables.')

    return allRowsIdentical
# compareTableData

# main
def main():
    try:
        global LOGGER, DB_CONFIG_FILE_PATH, tables, columnsToCompare

        initLogger()

        args = getCmdArgs()

        table1 = args.table1
        table2 = args.table2
        colsToCompare = args.cols
        config = args.config

        if table1 and table2:
            table1Split = table1.split('.')
            table2Split = table2.split('.')

            if config:
                DB_CONFIG_FILE_PATH = config

            if len(table1Split) == 2:
                if len(table2Split) == 2:
                    tables = [
                        {
                            'db': table1Split[0],
                            'table': table1Split[1],
                        },
                        {
                            'db': table2Split[0],
                            'table': table2Split[1],
                        }
                    ]

                    if colsToCompare:
                        # String to List
                        colsToCompare = [ tempColToCompare.strip() for tempColToCompare in colsToCompare.split(',') ]
                        # Remove empty Strings from List
                        columnsToCompare = [ tempColToCompare for tempColToCompare in colsToCompare if tempColToCompare ]

                    initDBConn()

                    allColumnsMatched = compareTableDefs()

                    if allColumnsMatched:
                        compareTableData()
                else:
                    LOGGER.error('table2 must be in format: db.table_name')
            else:
                LOGGER.error('table1 must be in format: db.table_name')
        else:
            LOGGER.error('Invalid Arguments! Arguments: table1, table2 are required')
            quit()
    except Exception as e:
        print('Error occurred:')
        traceback.print_exc()
        quit()
# main

if __name__ == '__main__':
    main()