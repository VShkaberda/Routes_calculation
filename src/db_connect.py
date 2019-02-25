# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 09:43:48 2018

@author: v.shkaberda
"""

import pyodbc


class DBConnect(object):
    ''' Provides connection to database and functions to work with server.
    '''
    def __init__(self, *, server, db, table_suffix=''):
        self._server = server
        self._db = db
        self.t_suff = table_suffix

    def __enter__(self):
        # Connection properties
        conn_str = (
            'Driver={{SQL Server}};'
            'Server={0};'
            'Database={1};'
            'Trusted_Connection=yes;'.format(self._server, self._db)
            )
        self.__db = pyodbc.connect(conn_str)
        self.__cursor = self.__db.cursor()
        return self

    def __exit__(self, type, value, traceback):
        self.__db.close()

    def count_empty_rows(self):
        ''' Count rows that have to be updated.
        '''
        query = "select count(*) \
                from dbo.rva_GeoMatrix" + self.t_suff + " mx \
                where mx.YN_Distance is NULL"
        self.__cursor.execute(query)
        return self.__cursor.fetchone()[0]

    def raw_query(self, query):
        ''' Takes the query and returns output from db.
        '''
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def empty_dist(self):
        ''' Fetch one row with empty distance from db.
        '''
        query = "select top 1 mx.id, mx.pointA, a.lat, a.lon, mx.pointB, b.lat, b.lon \
                from dbo.rva_GeoMatrix" + self.t_suff + " mx \
                    join dbo.rva_GeoMain" + self.t_suff + " a on mx.pointA = a.pointID \
                    join dbo.rva_GeoMain" + self.t_suff + " b on mx.pointB = b.pointID \
                where mx.YN_Distance is NULL \
                order by mx.id"
        self.__cursor.execute(query)
        return self.__cursor.fetchone()

    def update_dist(self, id_, km_time):
        ''' Update distance and time for row with id = id_.
        '''
        query = "UPDATE dbo.rva_GeoMatrix" + self.t_suff + " \
                SET YN_Distance = ?,YN_Time = ?, \
                YN2_Distance = ?, YN2_Time = ? WHERE id = ?"
        self.__cursor.execute(query,
                              km_time[1].km, km_time[1].time,
                              km_time[0].km, km_time[0].time, id_)
        self.__db.commit()


if __name__ == '__main__':
    with DBConnect(server='s-kv-center-s64', db='CB') as sql:
        query = 'select 42'
        assert sql.raw_query(query)[0][0] == 42, 'Server returns no output.'
    print('Connected successfully.')
    input('Press Enter to exit...')
