'''
Install Dependencies:
    - pip install mysql-connector-python rich
'''

import mysql.connector, logging, traceback, argparse, configparser, os
from typing import Final
from rich.console import Console as richConsole
from rich.table import Table as richTable

DB_CONFIG_FILE_PATH = 'db.config'

LOGGER = None
DB1_CONN = None
DB2_CONN = None

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

    argumentParser.add_argument('-db1', help = 'Database - 1')
    argumentParser.add_argument('-db2', help = 'Database - 2')

    argumentParser.add_argument('-ignore', help = 'Ignore Changes - Comma separated values.\nList of Values: comment, charset, dataLength, dataType, defaultValue, nullable')

    argumentParser.add_argument('-config', help = 'Path to Database config file')

    return argumentParser.parse_args()
# getCmdArgs

# initDBConn
def initDBConn(db1, db2):
    global DB1_CONN, DB2_CONN, DB_CONFIG_FILE_PATH, LOGGER

    defaultDBConfig: Final = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': 'letmein1!',
    }
    DB1_CONFIG = None
    DB2_CONFIG = None

    # Get DB credentials from config file
    if os.path.isfile(DB_CONFIG_FILE_PATH):
        # Read DB config file
        with open(f'{DB_CONFIG_FILE_PATH}', 'r') as fpDBConfig:
            dbConfigText = '[DEFAULT]\n' + fpDBConfig.read()

            config = configparser.ConfigParser(allow_no_value = True)
            config.read_string(dbConfigText)

            # if DB credentials found
            if config.has_option('DEFAULT', 'DB_USERNAME') or config.has_option('DEFAULT', 'DB_PASSWORD'):
                DB1_CONFIG = {
                    'host': config.get('DEFAULT', 'DB_HOST') if config.has_option('DEFAULT', 'DB_HOST') else defaultDBConfig.get('host'),
                    'port': config.get('DEFAULT', 'DB_PORT') if config.has_option('DEFAULT', 'DB_PORT') else defaultDBConfig.get('port'),
                    'database': db1,
                    'user': config.get('DEFAULT', 'DB_USERNAME'),
                    'password': config.get('DEFAULT', 'DB_PASSWORD'),
                }

                # Assign DB1 config to DB2
                for k, v in DB1_CONFIG.items():
                    if k == 'database':
                        DB2_CONFIG['database'] = db2
                    else:
                        DB2_CONFIG[k] = v

                LOGGER.info(f'Using same Credentials for db1, db2 from {DB_CONFIG_FILE_PATH}...')
            else:
                # if DB1 credentials found
                if config.has_option('DEFAULT', 'DB1_USERNAME') or config.has_option('DEFAULT', 'DB1_PASSWORD'):
                    DB1_CONFIG = {
                        'host': config.get('DEFAULT', 'DB1_HOST') if config.has_option('DEFAULT', 'DB1_HOST') else defaultDBConfig.get('host'),
                        'port': config.get('DEFAULT', 'DB1_PORT') if config.has_option('DEFAULT', 'DB1_PORT') else defaultDBConfig.get('port'),
                        'database': db1,
                        'user': config.get('DEFAULT', 'DB1_USERNAME'),
                        'password': config.get('DEFAULT', 'DB1_PASSWORD'),
                    }

                    LOGGER.info(f'Using db1 credentials from {DB_CONFIG_FILE_PATH}...')

                # if DB2 credentials found
                if config.has_option('DEFAULT', 'DB2_USERNAME') or config.has_option('DEFAULT', 'DB2_PASSWORD'):
                    DB2_CONFIG = {
                        'host': config.get('DEFAULT', 'DB2_HOST') if config.has_option('DEFAULT', 'DB2_HOST') else defaultDBConfig.get('host'),
                        'port': config.get('DEFAULT', 'DB2_PORT') if config.has_option('DEFAULT', 'DB2_PORT') else defaultDBConfig.get('port'),
                        'database': db2,
                        'user': config.get('DEFAULT', 'DB2_USERNAME'),
                        'password': config.get('DEFAULT', 'DB2_PASSWORD'),
                    }

                    LOGGER.info(f'Using db2 credentials from {DB_CONFIG_FILE_PATH}...')
        # Read DB config file
    else:
        # Assign Default DB config
        DB1_CONFIG = defaultDBConfig
        DB2_CONFIG = defaultDBConfig

        DB1_CONFIG['database'] = db1
        DB2_CONFIG['database'] = db2

        LOGGER.warning('No config file found! Using default credentials...')
    # Get DB credentials from config file

    if DB1_CONFIG:
        DB1_CONN = mysql.connector.connect(
            host = DB1_CONFIG.get('host'),
            port = DB1_CONFIG.get('port'),
            database = DB1_CONFIG.get('database'),
            user = DB1_CONFIG.get('user'),
            password = DB1_CONFIG.get('password'),
        )
    else:
        LOGGER.error('db1 Credentials not available!')
        quit()

    if DB2_CONFIG:
        DB2_CONN = mysql.connector.connect(
            host = DB2_CONFIG.get('host'),
            port = DB2_CONFIG.get('port'),
            database = DB2_CONFIG.get('database'),
            user = DB2_CONFIG.get('user'),
            password = DB2_CONFIG.get('password'),
        )
    else:
        LOGGER.error('db2 Credentials not available!')
        quit()
# initDBConn

# runQuery
def runQuery(dbConn, query, params = {}, toList = False):
    dbCursor = dbConn.cursor(dictionary = True)
    dbCursor.execute(query, params)

    result = dbCursor.fetchall()
    dbCursor.close()

    if toList:
        tempResult = result
        result = []

        for row in tempResult:
            result.append(row.get(toList))

    return result
# runQuery

# printInTableFormat
# @params headers -> List
# @params rows -> List[Tuple]
def printInTableFormat(headers, rows, title = None):
    rt = richTable(title = title, show_lines = True, title_justify = True)

    for header in headers:
        rt.add_column(header)

    for row in rows:
        rt.add_row(* row)

    rc = richConsole()
    rc.print(rt)

    # Empty print for log readability
    print('')
# printInTableFormat

# formatColumnToDisplay
def formatColumnToDisplay(col, constraints, ignoreDetails = []):
    charLength = col.get('CHARACTER_MAXIMUM_LENGTH')
    colDefault = col.get('COLUMN_DEFAULT')
    dispFormat = ''

    if not('dataType' in ignoreDetails):
        dispFormat = col.get('DATA_TYPE').decode() + '' + (f'({charLength})' if charLength and charLength > 0 else '')

    if not('charset' in ignoreDetails):
        dispFormat += (' CHARACTER SET ' + col.get('CHARACTER_SET_NAME') + (' COLLATE ' + col.get('COLLATION_NAME') if col.get('COLLATION_NAME') else '') if col.get('CHARACTER_SET_NAME') else '')

    if not('defaultValue' in ignoreDetails) and colDefault:
        dispFormat += f' DEFAULT \'{colDefault.decode()}\''

    if not('nullable' in ignoreDetails):
        dispFormat += (' NOT NULL' if col.get('IS_NULLABLE') == 'NO' else ('' if colDefault else ' DEFAULT') + ' NULL')

    if not('comment' in ignoreDetails) and col.get('COLUMN_COMMENT'):
        comment = col.get('COLUMN_COMMENT').decode()

        if comment and len(comment) > 0:
            dispFormat += f' COMMENT \'{comment}\''

    if not('constraints' in ignoreDetails) and constraints and len(constraints) > 0:
        for constraint in constraints:
            dispFormat += f'\n{constraint}'

    return dispFormat
# formatColumnToDisplay

# fetchTableDetails
def fetchTableDetails(dbConn, dbName, tableName):
    columnsQuery = 'SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = %(database)s AND TABLE_NAME = %(table)s ORDER BY COLUMN_NAME'

    columns = runQuery(dbConn, columnsQuery, { 'database': dbName, 'table': tableName })

    constraintsQuery = 'SELECT kcu.COLUMN_NAME, tc.CONSTRAINT_TYPE, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE kcu INNER JOIN information_schema.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME WHERE kcu.TABLE_SCHEMA = %(database)s AND kcu.TABLE_NAME = %(table)s'

    rawConstraints = runQuery(dbConn, constraintsQuery, { 'database': dbName, 'table': tableName })
    constraints = {}

    if rawConstraints and len(rawConstraints) > 0:
        for tempConstraint in rawConstraints:
            constraintName = tempConstraint.get('CONSTRAINT_TYPE')

            if tempConstraint.get('REFERENCED_TABLE_NAME'):
                constraintName += '(' + tempConstraint.get('REFERENCED_TABLE_NAME') + '.' + tempConstraint.get('REFERENCED_COLUMN_NAME') + ')'

            if tempConstraint.get('COLUMN_NAME') in constraints:
                constraints[ tempConstraint.get('COLUMN_NAME') ].append( constraintName )
            else:
                constraints[ tempConstraint.get('COLUMN_NAME') ] = [ constraintName ]

    return {
        'columns': columns,
        'constraints': constraints,
    }
# fetchTableDetails

# compareTableDetails
def compareTableDetails(db1, db2, ignoreDetails, tableName):
    global LOGGER, DB1_CONN, DB2_CONN
    areTablesIdentical = True

    LOGGER.info(f'Collecting {tableName} details from DB1...')

    db1TableDetails = fetchTableDetails(DB1_CONN, db1, tableName) 
    db1Columns = db1TableDetails.get('columns')
    db1TblConstraints = db1TableDetails.get('constraints')

    LOGGER.info(f'Collecting {tableName} details from DB2...\n')

    db2TableDetails = fetchTableDetails(DB2_CONN, db2, tableName)
    db2Columns = db2TableDetails.get('columns')
    db2TblConstraints = db2TableDetails.get('constraints')

    printHeader = ['Column', 'In DB1', 'In DB2']
    printData = []

    # Iterate DB1 Columns to Compare
    for db1Col in db1Columns:
        db1ColName = db1Col.get('COLUMN_NAME')
        db1ColConstraints = db1TblConstraints[db1ColName] if db1ColName in db1TblConstraints else []
        db1ColDispFormat = formatColumnToDisplay(db1Col, db1ColConstraints, ignoreDetails = ignoreDetails)
        db2Col = None

        # Check if column is present in db2.table
        for tempDb2Col in db2Columns:
            if tempDb2Col.get('COLUMN_NAME') == db1ColName:
                db2Col = tempDb2Col
                break
        # Check if column is present in db2.table

        if db2Col:
            db2ColName = db2Col.get('COLUMN_NAME')
            db2ColConstraints = db2TblConstraints[db2ColName] if db2ColName in db2TblConstraints else []
            db2ColDispFormat = formatColumnToDisplay(db2Col, db2ColConstraints, ignoreDetails = ignoreDetails)

            if db1ColDispFormat != db2ColDispFormat:
                printData.append((db2ColName, db1ColDispFormat, db2ColDispFormat))
                areTablesIdentical = False
        else:
            printData.append((db1ColName, db1ColDispFormat, 'NOT EXISTS'))
            areTablesIdentical = False
    # Iterate DB1 Columns to Compare

    # Iterate DB2 Columns to Compare
    for db2Col in db2Columns:
        db2ColName = db2Col.get('COLUMN_NAME')
        db2ColConstraints = db2TblConstraints[db2ColName] if db2ColName in db2TblConstraints else []
        db2ColDispFormat = formatColumnToDisplay(db2Col, db2ColConstraints, ignoreDetails = ignoreDetails)
        db1Col = None

        # Check if column is present in db1.table
        for tempDb1Col in db1Columns:
            if tempDb1Col.get('COLUMN_NAME') == db2ColName:
                db1Col = tempDb1Col
                break
        # Check if column is present in db1.table

        if db1Col:
            pass
        else:
            printData.append((db2ColName, 'NOT EXISTS', db2ColDispFormat))
            areTablesIdentical = False
    # Iterate DB2 Columns to Compare

    if not areTablesIdentical:
        printInTableFormat(printHeader, printData, title = tableName)

    return areTablesIdentical
# compareTableDetails

# compareDBs
def compareDBs(db1, db2, ignoreDetails = []):
    global LOGGER, DB1_CONN, DB2_CONN
    tablesListQuery = 'SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = %(database)s'

    db1Tables = runQuery(DB1_CONN, tablesListQuery, params = { 'database': db1 }, toList = 'TABLE_NAME')
    db2Tables = runQuery(DB2_CONN, tablesListQuery, params = { 'database': db2 }, toList = 'TABLE_NAME')

    tablesNoInDb1 = []
    tablesNoInDb2 = []
    commonTables = []

    for tableName in db1Tables:
        if tableName in db2Tables:
            commonTables.append(tableName)
        else:
            tablesNoInDb2.append(tableName)

    for tableName in db2Tables:
        if not(tableName in db1Tables):
            tablesNoInDb1.append(tableName)

    noMissingTables = (len(tablesNoInDb1) == 0 and len(tablesNoInDb2) == 0)

    if noMissingTables:
        LOGGER.info('All Tables are present in both the Databases.')
    else:
        printInTableFormat([
            'Tables Not presented in DB1',
            'Tables Not presented in DB2'
        ], [(
            ', '.join(tablesNoInDb1),
            ', '.join(tablesNoInDb2)
        )])

    commonTablesCount = len(commonTables)
    identicalTablesCount = 0

    if commonTablesCount > 0:
        if not noMissingTables:
            if commonTablesCount > 1:
                LOGGER.info(f'There are {commonTablesCount} Tables are present in both the Databases. Comparing the Tables...')
            else:
                LOGGER.info('There is a Table present in both the Databases. Comparing the Table...')

        # Empty print for log readability
        print('')

        # Iterate Tables to Compare
        for tableName in commonTables:
            areTablesIdentical = compareTableDetails(db1, db2, ignoreDetails, tableName)

            if areTablesIdentical:
                LOGGER.info(f'{tableName} Tables Details in both the Database are same.')
                identicalTablesCount += 1
        # Iterate Tables to Compare

        if noMissingTables:
            if commonTablesCount == identicalTablesCount:
                LOGGER.info('Hooray! Both the Databases are Identical.')

                return True
    else:
        LOGGER.info('There are no Identical Tables between both the Databases!')
        quit()
# compareDBs

# main
def main():
    try:
        global LOGGER, DB_CONFIG_FILE_PATH

        initLogger()

        args = getCmdArgs()

        db1 = args.db1
        db2 = args.db2
        ignore = args.ignore
        config = args.config

        if db1 and db2:
            if config:
                DB_CONFIG_FILE_PATH = config

            initDBConn(db1, db2)

            if ignore:
                ignore = [ tempIgnoreVal.strip() for tempIgnoreVal in ignore.split(',') ]
            else:
                ignore = []

            # Empty print for log readability
            print('')

            compareDBs(db1, db2, ignoreDetails = ignore)
        else:
            LOGGER.error('Invalid Arguments! Arguments: db1, db2 are required')
            quit()
    except Exception as e:
        print('Error occurred:')
        traceback.print_exc()
        quit()
# main

if __name__ == '__main__':
    main()