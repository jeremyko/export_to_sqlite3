#include "export_mysql_handler.hpp"
#include "common.hpp"

#include "export_mysql_handler.hpp"
#include "common.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>

extern string g_str_sqlite_file ;

#define MAX_MYSQL_MAPPING_CNT 23
T_DATA_TYPE_MAP g_MysqlDataTypeMappingTable[MAX_MYSQL_MAPPING_CNT] =
{
    //mysql -> sqlite
    { "INT","INTEGER" } ,
    { "VARCHAR","TEXT" } ,
    { "CHAR","TEXT" } ,
    { "TEXT","TEXT" } ,
    { "TINYINT","INTEGER" } ,
    { "DOUBLE","REAL" } ,
    { "BIGINT","INTEGER" } ,
    { "DATETIME","TEXT" } ,
    { "SMALLINT","INTEGER" } ,

    { "MEDIUMINT","INTEGER" } ,
    { "BIT","INTEGER" } ,
    { "BOOLEAN","INTEGER" } ,
    { "FLOAT","REAL" } ,
    { "DECIMAL","REAL" } ,
    { "NUMERIC","REAL" } ,
    { "DATE","TEXT" } ,
	//{ "DATE","DATETIME" } ,
    { "TIME","TEXT" } ,
    { "DATETIME","TEXT" } ,
    { "LONGTEXT","TEXT" } ,
    { "MEDIUMTEXT","TEXT" },
    { "BLOB","TEXT" } ,
    { "BIT","INTEGER" } ,
    { "YEAR","INTEGER" } 
};


ExportMysqlHandler::ExportMysqlHandler( string&  _user, 
										string&  _pass_wd, 
										string&  _ip, 
										int  _port, 
										string&  _db_name )
{
    user_ = _user;
    pass_wd_ = _pass_wd;
    ip_ = _ip ;
    port_ = _port ;
    db_name_ = _db_name ;
    mysql_handler_ = NULL;
}


ExportMysqlHandler::~ExportMysqlHandler()
{
	Terminate();

}

//sqlite callback
/*
static int sqlite_callback(void *NotUsed, int argc, char **argv, char **azColName)
{
	NotUsed = 0;
	int i;
	for (i = 0; i < argc; i++)
	{
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("callback\n");
	return 0;
}
*/
///////////////////////////////////////////////////////////////////////////////
char* ExportMysqlHandler::ConvertDataType( const char* szFrom ) 
{
    for( int i = 0; i < MAX_MYSQL_MAPPING_CNT; i++)
    {
        if( 0 == strncasecmp(szFrom, 
		                     g_MysqlDataTypeMappingTable [i].szDataTypeFrom , 
		                     strlen(g_MysqlDataTypeMappingTable [i].szDataTypeFrom) ) )
        {
            //printf("Convert [%s] ==> [%s]\n", szFrom, gDataTypeMappingTable [i].szDataTypeTo );
            return g_MysqlDataTypeMappingTable [i].szDataTypeTo ;
        }
    }
    printf("Error while converting Data Type [%s]\n", szFrom  );
    return NULL;
}
////////////////////////////////////////////////////////////////////////////////
bool ExportMysqlHandler::ConnectDB() 
{
    sqlite3_initialize( );

    int rst = sqlite3_open( g_str_sqlite_file.c_str() , &pSQLite3);

    if ( rst )
    {
        cout<< "Can't open database: "<< sqlite3_errmsg( pSQLite3 ) <<"\n" ;

        sqlite3_close( pSQLite3 );
        pSQLite3 = NULL;
        return false;
    }

	cout<< "sqlite Database opened : " << g_str_sqlite_file<<"\n";

    if( mysql_handler_ == NULL) 
    {
        mysql_handler_ = new MYSQL;
        if(mysql_handler_ == NULL) 
        {
            //WRITE_COM_ERRLOG(E_MEM_ALLOC, "[ExportMysqlHandler::ConnectDB] Fail to allocate memory");
            return false;
        }

        mysql_init(mysql_handler_);

        //mysql_options(mysql_handler_,MYSQL_OPT_RECONNECT,&m_bReconnect);                     // Reconnect 설정
        //mysql_options(mysql_handler_,MYSQL_OPT_CONNECT_TIMEOUT,(char*)&m_nDBTimeout);        // Connect Timeout 설정
        //mysql_options(mysql_handler_,MYSQL_OPT_READ_TIMEOUT,   (char*)&m_nReadTimeout);      // Read Timeout 설정
        //mysql_options(mysql_handler_,MYSQL_OPT_WRITE_TIMEOUT,  (char*)&m_nWriteTimeout);     // Write Timeout 설정

        if(!mysql_real_connect(	mysql_handler_, 
								ip_.c_str(), 
								user_.c_str(), 
								pass_wd_.c_str(), 
								db_name_.c_str(), 
								port_,
								(char *)NULL,0) )
        {
            printf("Error [%d_%s]\n",mysql_errno(mysql_handler_),mysql_error(mysql_handler_));

            delete mysql_handler_;
            mysql_handler_ = NULL;

            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
bool ExportMysqlHandler::BuildSchemaScript() 
{
	printf("\n** Creating Schema Script --> Start\n");

	char   cmdStr[256];
	/////////////////////////////////////////////////////////////////////////////
	//remove existing script
	memset(&cmdStr, 0x00, sizeof(cmdStr) );
	snprintf(cmdStr, sizeof(cmdStr), "rm -f sqlite_schema_script.sql" );
	system(cmdStr) ;

    string strSqlite = "";

    MYSQL_RES  *sqlResult ;
    MYSQL_ROW   row ;
    char        query[1024] ;

    sprintf(query,
		"SELECT A.TABLE_NAME, "
		"		A.COLUMN_NAME, "
		"		A.DATA_TYPE , "
		"		A.IS_NULLABLE , "
		"		A.ORDINAL_POSITION  "
		"FROM   INFORMATION_SCHEMA.COLUMNS  A "
		"WHERE  A.TABLE_SCHEMA = '%s' "
		"ORDER BY A.TABLE_NAME, A.ORDINAL_POSITION ", db_name_.c_str()
        
    );

    if ( mysql_query(mysql_handler_, query) != 0 )
    {
        printf("[%s-%d]  select error!(%d-%s)\n",
            __FILE__,__LINE__,mysql_errno(mysql_handler_),mysql_error(mysql_handler_));

        snprintf(errmsg_, sizeof(errmsg_), "select error!(%d-%s)\n",
            mysql_errno(mysql_handler_),mysql_error(mysql_handler_) ) ;

        return false;
    }
    else
    {
        sqlResult = mysql_store_result(mysql_handler_) ;

        bool bFirstFlag = true;
        string tbl_name = "";
        string col_name = "";
        string data_type = "";
        string nullable = "";
        int col_id = 0 ;

        //int data_length = 0;
        //string query_str = "";
        //
        int nExColCnt = 0;
        string ex_query_str = "";
        string ex_tbl_name = "";

        if ( mysql_num_rows(sqlResult) > 0 )
        {
            printf("\n** Data Exists\n");
            while( (row = mysql_fetch_row(sqlResult)) != NULL )
            {
				/*
                MYSQL_TYPE_TINY TINYINT field
                MYSQL_TYPE_SHORT    SMALLINT field
                MYSQL_TYPE_LONG INTEGER field
                MYSQL_TYPE_INT24    MEDIUMINT field
                MYSQL_TYPE_LONGLONG BIGINT field
                MYSQL_TYPE_DECIMAL  DECIMAL or NUMERIC field
                MYSQL_TYPE_NEWDECIMAL   Precision math DECIMAL or NUMERIC field (MySQL 5.0.3 and up)
                MYSQL_TYPE_FLOAT    FLOAT field
                MYSQL_TYPE_DOUBLE   DOUBLE or REAL field
                MYSQL_TYPE_BIT  BIT field (MySQL 5.0.3 and up)
                MYSQL_TYPE_TIMESTAMP    TIMESTAMP field
                MYSQL_TYPE_DATE DATE field
                MYSQL_TYPE_TIME TIME field
                MYSQL_TYPE_DATETIME DATETIME field
                MYSQL_TYPE_YEAR YEAR field
                MYSQL_TYPE_STRING   CHAR or BINARY field
                MYSQL_TYPE_VAR_STRING   VARCHAR or VARBINARY field
                MYSQL_TYPE_BLOB BLOB or TEXT field (use max_length to determine the maximum length)
                MYSQL_TYPE_SET  SET field
                MYSQL_TYPE_ENUM ENUM field
                MYSQL_TYPE_GEOMETRY Spatial field
                MYSQL_TYPE_NULL NULL-type field
                */ 

                //MYSQL_FIELD
                //MYSQL_TYPE_TINY

                tbl_name    = row[0];
                col_name    = row[1];
                data_type   = row[2];
                nullable    = row[3];
                col_id      = atoi(row[4]);

                //data_length = atoi(row[3]);
                //query_str   = row[5];

                //unsigned long *lengths;
                //lengths = mysql_fetch_lengths(SqlResult);
                //for(i = 0; i < num_fields; i++)
                //{
                //    printf("[%.*s] ", (int) lengths[i], row[i] ? row[i] : "NULL");
                //}
                /*
                MYSQL_FIELD *field;
                while((field = mysql_fetch_field(sqlResult)))
                {
                    //http://dev.mysql.com/doc/refman/5.0/en/c-api-data-structures.html
                    printf("field name %s\n", field->name);//type
					if (IS_NUM(field->type))
					{
						printf("Field is numeric\n");
					}
                }
				*/
                //printf("Data Type[%s]\n", data_type.c_str() );

                if( col_id == 1)
                {
                    if(bFirstFlag )
                    {
                        bFirstFlag = false; 
                    }
                    else
                    {
                        strSqlite += "\n);\n\n" ;

                        T_QueryPerTable QueryInfo;
                        QueryInfo.strTableName  = ex_tbl_name ;
                        QueryInfo.strTableQuery = ex_query_str ;
                        QueryInfo.nColCnt = nExColCnt ;
                        vecQuery.push_back(QueryInfo);
                    }

                    strSqlite += "\nCREATE TABLE " + tbl_name + "(\n" ;
                    strSqlite += "\t"+col_name +"\t\t\t";
                    if( NULL == ConvertDataType( data_type.c_str()  ) )
                    {
                        printf("Failed to Convert Data Type !!\n");
                        mysql_free_result(sqlResult) ;  
                        return false;
                    }

                    strSqlite += ConvertDataType( data_type.c_str()  );
                }
                else
                {
                    nExColCnt = col_id;
                    ex_tbl_name = tbl_name ;
                    //ex_query_str = query_str ;

                    strSqlite += "\t,\n" ;
                    strSqlite += "\t"+col_name+"\t\t\t" ;
                    if( NULL == ConvertDataType( data_type.c_str()  ) )
                    {
                        printf("Failed to Convert Data Type !!\n");
                        mysql_free_result(sqlResult) ; 
                        return false;
                    }
                    strSqlite += ConvertDataType( data_type.c_str()  );
                }
                char tempBuff[100];
                sprintf(tempBuff, "%s_%d", tbl_name.c_str() , col_id );
                std::string strKey = tempBuff;

                T_DataTypeInfo tmpTypeInfo;
                tmpTypeInfo.strColName = col_name;
                tmpTypeInfo.strDataType = data_type;
                //tmpTypeInfo.nNumberLength = data_length;

                std::pair<std::map<string,T_DataTypeInfo>::iterator,bool> ret;
                ret = mapColToType.insert ( std::pair<string,T_DataTypeInfo>( strKey, tmpTypeInfo ) );
            } //while

            strSqlite += "\n);\n" ;
            // insert into vector ==> query_str
            T_QueryPerTable QueryInfo;
            QueryInfo.strTableName  = ex_tbl_name ;
            QueryInfo.strTableQuery = ex_query_str ;
            QueryInfo.nColCnt = nExColCnt ;
            vecQuery.push_back(QueryInfo);
            //printf("strSqlite [%s]\n", strSqlite.c_str() );

            /////////////////////////////////////////////////////////////////////////////
            // build schema script

            //remove existing db
            memset(&cmdStr, 0x00, sizeof(cmdStr) );
            snprintf(cmdStr, sizeof(cmdStr), "rm -f %s", g_str_sqlite_file.c_str() );
            system(cmdStr) ;
            //printf("\n**  %s\n", cmdStr);

            FILE * outScript;
            outScript = fopen("sqlite_schema_script.sql", "w"); 
            fprintf(outScript, strSqlite.c_str() );
            fclose(outScript); 
            printf("\n** Creating Schema Script --> Done\n");
        }
		else
		{
            printf("\n** No Data Exists\n");
		}

        mysql_free_result(sqlResult) ;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
bool ExportMysqlHandler::FillSqlite() 
{

    //sqlite table is created after first open, re-open it
	int rst = sqlite3_open( g_str_sqlite_file.c_str() , &pSQLite3);

	if ( rst )
	{
		cout<< "Can't open database: "<< sqlite3_errmsg( pSQLite3 ) <<"\n" ;
		sqlite3_close( pSQLite3 );
		pSQLite3 = NULL;
		return false;
	}

    cout << "\n** Creating Fill Script --> Start" << endl;
    MYSQL_RES  *sqlResult ;
    MYSQL_ROW   row ;

    char cmdStr[256];
    memset(&cmdStr, 0x00, sizeof(cmdStr) );
    snprintf(cmdStr, sizeof(cmdStr), "rm -f sqlite_fill_script.sql" );
    system(cmdStr) ;

    string strFillScript = "";

	char    *szErrMsg = NULL; 
	sqlite3_exec(pSQLite3, "BEGIN", 0, 0, 0); //transaction

    vector<T_QueryPerTable>::iterator end = vecQuery.end();

    for( vector<T_QueryPerTable>::iterator itor = vecQuery.begin(); itor != end; ++itor)
    {
        T_QueryPerTable QueryInfo = *itor;
        cout << " -- table [" << QueryInfo.strTableName << "] " << " Column Cnt [" << QueryInfo.nColCnt << "]"<<endl;
        
		char tmpSql [256];
		memset(&tmpSql, 0x00, sizeof(tmpSql));
		snprintf(tmpSql, sizeof(tmpSql), "SELECT * FROM %s", QueryInfo.strTableName.c_str() );

		if ( mysql_query(mysql_handler_, tmpSql) != 0 )
		{
			printf("[%s-%d]  select error!(%d-%s)\n",
					__FILE__,__LINE__,mysql_errno(mysql_handler_),mysql_error(mysql_handler_));

			snprintf(errmsg_, sizeof(errmsg_), "select error!(%d-%s)\n",
					mysql_errno(mysql_handler_),mysql_error(mysql_handler_) ) ;

			sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction
			sqlite3_close( pSQLite3);

			return false;
		}
		else
		{
			sqlResult = mysql_store_result(mysql_handler_) ;

			if ( mysql_num_rows(sqlResult) > 0 )
			{
				while( (row = mysql_fetch_row(sqlResult)) != NULL )
				{
					strFillScript += "INSERT INTO " + QueryInfo.strTableName + " ( " ;

					string str_insert_query =  "INSERT INTO " + QueryInfo.strTableName + " ( " ;

					//build cols..
					for (int i = 0; i < QueryInfo.nColCnt; i++)
					{
						char tempBuff[100];
						sprintf(tempBuff, "%s_%d", QueryInfo.strTableName.c_str() , i+1 );
						std::string strKey = tempBuff;

						T_DataTypeInfo dataTypeInfo = mapColToType.find ( strKey)->second ;

						if( i == 0)
						{
							strFillScript += dataTypeInfo.strColName ; 
							str_insert_query += dataTypeInfo.strColName ;
						}
						else
						{
							strFillScript += ", "+dataTypeInfo.strColName ; 
							str_insert_query  += ", "+dataTypeInfo.strColName ;
						}
					}

					strFillScript += " ) VALUES ( " ;
					str_insert_query += " ) VALUES ( " ;

					for (int i = 0; i < QueryInfo.nColCnt; i++)
					{

						char tempBuff[100];
						sprintf(tempBuff, "%s_%d", QueryInfo.strTableName.c_str() , i+1 );
						std::string strKey = tempBuff;

						//printf("key[%s] \n", tempBuff );

                        map <string, T_DataTypeInfo>::iterator  iter = mapColToType.find ( strKey);
						if( iter == mapColToType.end() )
						{
						    printf("[%d]error!: not found \n",__LINE__);
						}

						T_DataTypeInfo dataTypeInfo = iter->second ;

						//printf("key[%s] data col[%s] type [%s]\n", 
						//    tempBuff, dataTypeInfo.strColName.c_str(), dataTypeInfo.strDataType.c_str() );


						if  (
								dataTypeInfo.strDataType == "varchar" ||
								dataTypeInfo.strDataType == "text" ||
								dataTypeInfo.strDataType == "datetime" ||
								dataTypeInfo.strDataType == "date" ||
								dataTypeInfo.strDataType == "time" ||
								dataTypeInfo.strDataType == "timestamp" ||
								dataTypeInfo.strDataType == "longtext" ||
								dataTypeInfo.strDataType == "char" 
							)
						{
							//string strData = row[i];
							string strConverted = "";
							if( row[i] )
							{
							    string strData = row[i];
								strConverted = replaceSingleQuote( strData ) ;
							}

							if( i == 0)
							{
								strFillScript += "'"+ strConverted +"'" ; 
								str_insert_query += "'"+ strConverted +"'" ;
							}
							else
							{
								strFillScript += ", '"+ strConverted +"'" ; 
								str_insert_query += ", '"+ strConverted +"'" ;
							}

							//replace ' to ''
							//std::cout << "  string => " << strData << '\n';
						}
						else if ( 
						            dataTypeInfo.strDataType == "tinyint" ||
						            dataTypeInfo.strDataType == "int" ||
						            dataTypeInfo.strDataType == "bigint" ||
						            dataTypeInfo.strDataType == "smallint" ||
						            dataTypeInfo.strDataType == "mediumint" ||
						            dataTypeInfo.strDataType == "bit" ||
						            dataTypeInfo.strDataType == "decimal" ||
						            dataTypeInfo.strDataType == "float" ||
						            dataTypeInfo.strDataType == "year" ||
						            dataTypeInfo.strDataType == "double" 
						        )
						{
							string tmpData =  "";
							if( row[i] )
							{
                                tmpData = row[i];

								if( i == 0)
								{
									strFillScript += tmpData;
									str_insert_query += tmpData;
								}
								else
								{
									strFillScript += ", "+ tmpData;
									str_insert_query  += ", "+ tmpData;
								}
							}
							else
							{
								//std::cout << "  Number is NULL! "  << '\n';

								if( i == 0)
								{
									strFillScript += "NULL" ; 
									str_insert_query  += "NULL" ;
								}
								else
								{
									strFillScript += ", NULL"; 
									str_insert_query  += ", NULL";
								}
							}
						}
						else
						{
							std::cout << "Error : Undefined work for => " << dataTypeInfo.strDataType << '\n';
							sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction
							sqlite3_close( pSQLite3);

							return false;
						}
					} //for

					strFillScript += " );\n" ;
					str_insert_query += " )" ;
					//std::cout << "  Fill => " << str_insert_query << '\n';

					//sqlite api 

					int rslt = sqlite3_exec(pSQLite3, str_insert_query.c_str(),NULL,  0, &szErrMsg);

					if (rslt != SQLITE_OK)
					{
						printf ("Error [%s]\n", szErrMsg ); 
						sqlite3_free( szErrMsg );
						sqlite3_close( pSQLite3);
						sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction
						return false;
					}

				} // while( (row = mysql_fetch_row(sqlResult)) != NULL )

			} //if ( mysql_num_rows(sqlResult) > 0 )

		}//else

		mysql_free_result(sqlResult) ;

    } //for

	sqlite3_free( szErrMsg );
	sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction

    FILE * outScript;
    outScript = fopen("sqlite_fill_script.sql", "w"); 
    fprintf(outScript, strFillScript.c_str() );
    fclose(outScript); 

    cout << "\n** Creating Fill Script --> Done" << endl;

    return true;
}


////////////////////////////////////////////////////////////////////////////////
void ExportMysqlHandler::Terminate()
{
	sqlite3_close( pSQLite3);

    if(mysql_handler_ != NULL)
    {
        mysql_close(mysql_handler_);
        delete mysql_handler_;
        mysql_handler_ = NULL;
    }
}
