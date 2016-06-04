# -*- coding: utf-8 -*-
"""
Created on Thu Jun  2 14:03:46 2016

@author: AbreuLastra_Work
"""

import pandas as pd
import sqlite3 as lite

con = lite.connect('citi_bike.db')
cur = con.cursor()

df = pd.read_sql_query('SELECT * FROM available_bikes ORDER BY execution_time', con, index_col='execution_time')