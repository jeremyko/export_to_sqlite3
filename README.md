usage : export_to_sqlite SQLITE_FILE DB_TYPE DB_USER DB_PASSWD IP  PORT DB_NAME

       ex : from oracle 
            export_to_sqlite ofcs.sqlite ORACLE  OFCS  OFCS  192.168.1.225 1521 orcl

       ex : from Mysql
            export_to_sqlite ofcs.sqlite MYSQL   pcn   pcn   192.168.1.204 3306 PCN

auto generated scripts :
* sqlite_schema_script.sql ==> sqlite table schema generation script.
* sqlite_fill_script.sql   ==> sqlite table insert script.
