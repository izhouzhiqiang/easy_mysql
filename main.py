import pymysql
import tabulate
import enum
import configparser
import os
import sys

class OutputFormat(enum.Enum):
    GRID = 'grid'
    PLAIN = 'plain'
    SIMPLE = 'simple'
    FANCY_GRID = 'fancy_grid'
    PIPE = 'pipe'
    PRESTO = 'presto'
    ORGTBL = 'orgtbl'
    RST = 'rst'
    MEDIAWIKI = 'mediawiki'
    HTML = 'html'
    LATEX = 'latex'
    LATEX_RAW = 'latex_raw'
    LATEX_BOOKTABS = 'latex_booktabs'
    LATEX_LONGTABLE = 'latex_longtable'
    JIRA = 'jira'
# mysql -hmapguokepre0000-offline.xdb.all.serv -P4396 -uzhouzhiqiang04 -p'kqvjpt=wBr' -Dgzpre_guoke_dawn

# Output Format


class MySQLObject():
    def __init__(self):
        # 数据库信息
        self.hostname = ''
        self.username = ''
        self.password = ''
        self.database = ''
        self.port = 3306
        self.charset = 'utf8'

        # histroy sql & output
        self.sql_history = []
        self.result = None
        self.result_headers = None

        # Configure
        ## Output Format
        self.output_format = OutputFormat.GRID
        self.missingval = "NULL"
        self.debug = False

        # Process Info
        self.cwd = os.getcwd()
        self.bin_path = os.path.dirname(os.path.abspath(__file__))
        self.home_path = os.path.expanduser("~/.easy_mysql")
        self.config_filename = 'easy_mysql.ini'

        self.load_config()

        ## UI
        self.cmd_prefix = "{}@{}:{} >> ".format(self.username, self.hostname, self.database)

        # transction
        self.transction_depth = 0

        # connection
        self.conn = self._get_connection()
        self.cursor = self.conn.cursor()

    def load_config(self):
        filename = ""
        if os.path.exists(os.path.join(self.bin_path, self.config_filename)):
            filename = os.path.join(self.bin_path, self.config_filename)
        elif os.path.exists(os.path.join(self.home_path, self.config_filename)):
            filename = os.path.join(self.home_path, self.config_filename)
        else:
            print("No Config File Found.\n`{}` or `{}` should be set".format(
                os.path.join(self.bin_path, self.config_filename),
                os.path.join(self.home_path, self.config_filename)
            ))
            exit(0)

        try:
            config = configparser.ConfigParser()
            config.read(filename)
            self.hostname = config['database']['host']
            self.username = config['database']['username']
            self.password = config['database']['password']
            self.database = config['database']['database']
            self.port = int(config['database'].get('port'))
        except Exception as e:
            print("Error: {}".format(e))
            exit(0)


    def _get_connection(self):
        return pymysql.connect(
            host=self.hostname,
            user=self.username,
            password=self.password,
            database=self.database,
            port=self.port
        )

    def _exec_sql(self, sql):
        try:
            self.cursor.execute(sql)
            self.result = self.cursor.fetchall()
            self.result_headers = [x[0] for x in self.cursor.description]
            self._debug(self.result_headers)
            self._debug(type(self.result_headers))
            self.conn.commit()
        except Exception as e:
            raise e

    def Exec(self, sql):
        self._record_sql_histroy(sql)
        self._exec_sql(sql)

    def _record_sql_histroy(self, sql):
        self.sql_history.append(sql)

    def Print(self):
        self._debug(self.output_format.value)
        print(tabulate.tabulate(
            self.result,
            self.result_headers,
            self.output_format.value
        ))
    
    def _debug(self, msg):
        if self.debug:
            print("==Normal Debug : {}".format(msg))
    
    def Run(self):
        exec_sql = ""
        while(True):
            print("{} ".format(self.cmd_prefix), end="")
            sql = input().strip()

            if sql in ('exit', 'quit', '\q'):
                break
            elif sql == "":
                continue
            elif not sql.endswith(";"):
                exec_sql += sql + "\n"
                continue
            else:
                exec_sql += sql
                try:
                    self.Exec(exec_sql)
                    self.Print()
                except Exception as e:
                    print("ErrorCode: {}".format(e.args[0] if len(e.args) > 0 else str(e)))
                    print("ErrorMsg : {}".format(e.args[1] if len(e.args) > 1 else str(e)))
                exec_sql = ""

if __name__ == "__main__":
    mysql = MySQLObject()
    mysql.Run()

