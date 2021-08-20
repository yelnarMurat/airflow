# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
from builtins import chr
from collections import OrderedDict
import unicodecsv as csv
import logging
from tempfile import NamedTemporaryFile
import pymssql
 
 
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
 
 
class MsSqlToOracleTransfer(BaseOperator):
    
 
    template_fields = ('sql', 'partition', 'hive_table')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'
 
    @apply_defaults
    def __init__(
            self,
            sql,
            oracle_table,
            create=True,
            recreate=False,
            partition=None,
            delimiter=chr(1),
            mssql_conn_id='mssql_default',
            oracle_conn_id='oracle_conn_id',
            *args, **kwargs):
        super(MsSqlToOracleTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.oracle_table = oracle_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = delimiter
        self.mssql_conn_id = mssql_conn_id
        self.oracle_conn_id = oracle_conn_id
        self.partition = partition or {}
 
    @classmethod
    def type_map(cls, mssql_type):
        t = pymssql
        d = {
            t.BINARY.value: 'INT',
            t.DECIMAL.value: 'FLOAT',
            t.NUMBER.value: 'INT',
        }
        return d[mssql_type] if mssql_type in d else 'STRING'
 
    def execute(self, context):
        oracle = OracleHook(oracle_conn_id=self.oracle_conn_id)
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
 
        logging.info("Dumping Microsoft SQL Server query results to local file")
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
		
        if self.ora_preoperator:
            logging.info("Running Oracle preoperator")
            dest_ora.run(self.ora_preoperator)

        logging.info("Inserting rows into Oracle")

        dest_ora.insert_rows(table=self.ora_table, rows=cursor)

        if self.ora_postoperator:
            logging.info("Running Oracle postoperator")
            logging.info(self.ora_postoperator)
            src_ora.run(self.ora_postoperator)
            #conn = dest_ora.get_conn()
            #cursor = conn.cursor()
            #logging.info(self.ora_postoperator)
            #logging.info(self.parameters)
            #cursor.execute(self.ora_postoperator, self.parameters)
            #conn.commit;
            #cursor.close;
            #conn.close;
