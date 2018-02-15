# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 14:21:00 2017

@author: Matthew Berkeley
"""

import sqlite3
from sqlite3 import Error
import numpy as np
import sys
import pah_mapping
import matplotlib.pyplot as plt

def create_connection(db_file):
    """
    Create database connection to the SQLite database specified by db_file.
    :param db_file: database file
    :return: connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None
    
def create_table(conn, create_table_sql):
    """
    Create table from create_table_sql statement
    
    Parameters
    ----------
    
    conn : str
        connection object \n
    create_table_sql : str
        a CREATE TABLE statement \n
    
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
    return
        
def create_entry(conn, table_name, row_details, entry):
    """
    Create new entry for file in table
    
    Parameters
    ----------
    
    conn : str
        connection object \n
    entry : tuple
        row entry \n
    
    Return
    ------
    
    cursor.lastrowid:
        project id \n
    """
    sql = """ INSERT INTO {0}{1}
                VALUES({2})""".format(table_name, str(row_details), ",".join('?'*len(entry)))
    val = entry
    with conn:
        cur = conn.cursor()
        cur.execute(sql, val)
    return cur.lastrowid
    
    
def create_file_database(db_name, files):
    """
    Create a database containing all the files for a given input file list.
    
    Parameters
    ----------
    
    db_name : str
        database name \n
    files : str
        name of the text file containing all the WISE filenames \n
    
    """
    db_name = db_name.split("/")[-1]
    #database = '/Users/Laptop-23950/Documents/NASA/PAH_Project/sqlite_dbs/'+str(db_name)
    database = '/home/users/mberkeley/PAH_Project/sqlite_dbs/{}'.format(str(db_name))
    
    sql_create_files_table = """ CREATE TABLE IF NOT EXISTS files (
                                        id integer PRIMARY KEY,
                                        prefix text NOT NULL,
                                        band integer NOT NULL
                                    );"""
                                    
    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_files_table)
    else:
        print('Error! Cannot create database connection.')
    
    with open(files,'rb') as f:
        split_lines = f.read().splitlines()
        file_list = [x.split("/")[-1] for x in split_lines if 'int' in x]
    for i in xrange(len(file_list)):
        item = file_list[i]
        row_details = '(prefix, band)'
        entry = (str(item[:9]), str(item[11]))
        create_entry(conn, 'files', row_details, entry)
        pah_mapping.print_progress(i+1, len(file_list))
    
    return
    
def create_overlaps_table(db_name):
    #database = '/Users/Laptop-23950/Documents/NASA/PAH_Project/sqlite_dbs/'+str(db_name)
    if not '/' in db_name:
        db_name = '/home/users/mberkeley/PAH_Project/sqlite_dbs/{}'.format(str(db_name))
    elif './' in db_name:
        db_name = '/home/users/mberkeley/PAH_Project/sqlite_dbs/{}'.format(str(db_name).split('/')[-1])
    
    sql_create_overlaps_table = """CREATE TABLE IF NOT EXISTS overlaps (
                                    overlap_id integer PRIMARY KEY,
                                    file1_id integer NOT NULL,
                                    file2_id integer NOT NULL,
                                    background1 decimal,
                                    background2 decimal
                                );"""
    conn = create_connection(db_name)
    if conn is not None:
        create_table(conn, sql_create_overlaps_table)
        
    return
    
def add_column(db_name, table_name, new_column, definition):
    """
    Add a new column to a table in a database.
    
    Parameters
    ----------
    
    db_name : str
        Name of database \n
    table_name : str
        Name of table within database \n
    new_column : str
        Name of new column to add \n
    definition : str
        SQL string containing column definition (data type and NULL/NOT NULL etc)
        
    """
    conn = create_connection(db_name)
    sql_add_column = """ALTER TABLE {0} 
                            ADD {1} {2}
                            ;""".format(table_name, new_column, definition)
    print sql_add_column
    
    cur = conn.cursor()
    cur.execute(sql_add_column)
    
    return

def remove_column(db_name, table_name, drop_column):
    
    conn = create_connection(db_name)
    with conn:
        cur = conn.cursor()
    
        cur.execute("""SELECT * FROM {0}""".format(table_name))
        col_names = [str(i[0]) for i in cur.description]
        col_names.pop(np.where(np.char.strip(col_names) == str(drop_column))[0][0])
        sql_remove_column = """PRAGMA foreign_keys=off;
                                BEGIN TRANSACTION;
                                ALTER TABLE {0} RENAME TO _temp_table;
                                
                                CREATE TABLE {0} (
                                    {1}
                                    );
                                INSERT INTO {0} ({1})
                                    SELECT {1}
                                    FROM _temp_table
                                    ;
                                COMMIT;
                                PRAGMA foreign_keys=on""".format(table_name,', '.join(col_names))
                                
        cur.executescript(sql_remove_column)
    
    with conn:
        cur = conn.cursor()
        cur.execute("""DROP TABLE IF EXISTS {0};""".format(str('_temp_table')))
    return
    
def remove_table(db_name,drop_table):
    conn = create_connection(db_name)
    with conn:
        cur = conn.cursor()
        cur.execute("""DROP TABLE IF EXISTS {0};""".format(str(drop_table)))
    return
    
def remove_duplicates(db_name, table_name):
    conn = create_connection(db_name)
    
    sql_remove_duplicates = """DELETE FROM {0}
                                WHERE EXISTS (
                                    SELECT *
                                    FROM {0} AS t2
                                    WHERE {0}.file1_id = t2.file1_id
                                        AND {0}.file2_id = t2.file2_id
                                        AND {0}.overlap_id > t2.overlap_id
                                    );""".format(str(table_name))
    with conn:
        cur = conn.cursor()
        cur.execute(sql_remove_duplicates)
    
def update_entry(db_name, table_name, update, condition):
    conn = create_connection(db_name)
    with conn:
        cur = conn.cursor()
        sql_update_entry = """UPDATE {0}
                                SET {1}
                                WHERE {2}
                                ;""".format(table_name, update, condition)
        cur.execute(sql_update_entry)
    return
    
            
if __name__ == '__main__':
    db_name = sys.argv[1]
    table_name = sys.argv[2]
    #remove_duplicates(db_name, table_name)
    #files = sys.argv[2]
    #create_file_database(db_name, files)
    
    #files_txt = sys.argv[1]
    #band = sys.argv[2]
    #with open(files_txt, 'rb') as f:
    #    split_lines = f.read().splitlines()
    #    file_list = [x for x in split_lines if 'int' in x]
    #for filename in file_list:
    #    datafile = pah_mapping.File(filename)
    #    datafile.calibrate(band)
    #db_name = sys.argv[1]
    #remove_column(db_name, 'files','background')
    #remove_table(conn, '_temp_table')
    conn = create_connection(db_name)
    cur = conn.cursor()
    
    #conn_neighbor = sql_funcs.create_connection(neighbor_db_name)
    #cur_neighbor = conn_neighbor.cursor()    
    
    sql_saved_overlaps = """SELECT file1_id
                            FROM {0}
                            ;""".format(table_name)
    
    with conn:
        cur.execute(sql_saved_overlaps)
        file1_id = np.array([row[0] for row in cur.fetchall()])
        print file1_id
    
    plt.hist(file1_id[file1_id<100000], bins=1400)
    plt.show()