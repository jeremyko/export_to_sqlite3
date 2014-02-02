#include "export_oracle_handler.hpp"
#include "common.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern string g_str_sqlite_file ;

#define MAX_ORACLE_MAPPING_CNT 8

T_DATA_TYPE_MAP g_OracleDataTypeMappingTable[MAX_ORACLE_MAPPING_CNT] =
{
	//oracle -> sqlite
	{ "VARCHAR2","TEXT" } ,
	{ "NUMBER","INTEGER" } ,
	//{ "DATE","DATETIME" } ,
	{ "DATE","TEXT" } ,
	{ "CHAR","TEXT" } ,
	{ "FLOAT","REAL" } ,
	{ "LONG","TEXT" } ,
	{ "BINARY_DOUBLE","TEXT" } ,
	{ "TIMESTAMP","TEXT" } 
};

ExportOracleHandler::ExportOracleHandler(   string&  _user, 
											string&  _pass_wd, 
											string&  _ip, 
											int  _port, 
											string&  _db_name )
{
    env = 0;
    conn = 0;
    user_ = _user;
    pass_wd_ = _pass_wd;
    ip_ = _ip ;
    port_ = _port ;
    db_name_ = _db_name ;
}


ExportOracleHandler::~ExportOracleHandler()
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
	printf("\n");
	return 0;
}
*/
///////////////////////////////////////////////////////////////////////////////
char* ExportOracleHandler::ConvertDataType( const char* szFrom )
{
    for( int i = 0; i < MAX_ORACLE_MAPPING_CNT; i++)
    {
        //printf("[%s] [%d]\n", szFrom , strlen(gDataTypeMappingTable [i].szDataTypeFrom));
        if( 0 == strncmp(szFrom, g_OracleDataTypeMappingTable [i].szDataTypeFrom, 
            strlen(g_OracleDataTypeMappingTable [i].szDataTypeFrom)  ) )
        {
            //printf("Convert [%s] ==> [%s]\n", szFrom, g_OracleDataTypeMappingTable [i].szDataTypeTo );
            return g_OracleDataTypeMappingTable [i].szDataTypeTo ;
        }
    }
    printf("Error while converting Data Type [%s]\n", szFrom  );
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
bool ExportOracleHandler::ConnectDB() 
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

	cout<< "sqlite Database opened!!" << "\n";

    try
    {       
        env = Environment::createEnvironment(Environment::DEFAULT);     
        char temp_conn_string [256];
        memset(&temp_conn_string, 0x00, sizeof(temp_conn_string) );
		snprintf(temp_conn_string, sizeof(temp_conn_string), "%s:%d/%s", ip_.c_str(), port_, db_name_.c_str() );
      
        conn = env->createConnection( user_, pass_wd_, temp_conn_string );
    }
    catch (oracle::occi::SQLException ex)
    {
        printf("exception code (%d), %s\n", ex.getErrorCode(), ex.what());

        if (conn) 
        {
            env->terminateConnection(conn);
        }

        if (env) 
        {
            Environment::terminateEnvironment(env);
        }

        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
bool ExportOracleHandler::BuildSchemaScript() 
{
	printf("\n** Creating Schema Script --> Start\n");

	char   cmdStr[256];
	//remove existing script
	memset(&cmdStr, 0x00, sizeof(cmdStr) );
	snprintf(cmdStr, sizeof(cmdStr), "rm -f sqlite_schema_script.sql" );
	system(cmdStr) ;


    Statement *stmt = 0;
    ResultSet *rs = 0;
    string strSqlite = "";

    try
    {       
	    stmt = conn->createStatement(
			   "SELECT  B.TABLE_NAME, "
			   "        B.COLUMN_NAME, "
			   "        B.DATA_TYPE , "
			   "        B.DATA_LENGTH , "
			   "        B.NULLABLE , "
			   "        B.COLUMN_ID "
			   "FROM    USER_COL_COMMENTS    A, "
			   "        USER_TAB_COLUMNS     B  "
			   "WHERE  B.TABLE_NAME = A.TABLE_NAME " 
			   "AND    B.COLUMN_NAME = A.COLUMN_NAME " 
			   "ORDER BY B.TABLE_NAME, B.COLUMN_ID "
        );

        /* TODO create index 
        SELECT A.CONSTRAINT_NAME, A.TABLE_NAME, A.COLUMN_NAME
		FROM USER_CONS_COLUMNS  A
		WHERE OWNER ='OFCS'
		AND TABLE_NAME ='TEST_TBL'
		AND CONSTRAINT_NAME LIKE '%PK'
		;
        */
        rs = stmt->executeQuery();

        bool bFirstFlag = true;

        string query_str = "";
        string tbl_name = "";
        string col_name = "";
        string data_type = "";
        int data_length = 0;
        string nullable = "";
        int col_id = 0 ;

        int nExColCnt = 0;
        string ex_query_str = "";
        string ex_tbl_name = "";


        while ( rs->next() )
        {
            tbl_name = rs->getString(1);
            col_name = rs->getString(2);
            data_type = rs->getString(3);
            data_length = rs->getInt(4);
            nullable = rs->getString(5);
            col_id = rs->getInt(6);

            if( col_id == 1)
            {
                //printf("\t-------------------------------------------\n" );
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
                    stmt->closeResultSet(rs);
                    conn->terminateStatement(stmt);     
                    return false;
                }

                strSqlite += ConvertDataType( data_type.c_str()  );

            }
            else
            {
                nExColCnt = col_id;
                ex_tbl_name = tbl_name ;
                ex_query_str=query_str ;

                strSqlite += "\t,\n" ;
                strSqlite += "\t"+col_name+"\t\t\t" ;
                if( NULL == ConvertDataType( data_type.c_str()  ) )
                {
                    printf("Failed to Convert Data Type !!\n");
                    stmt->closeResultSet(rs);
                    conn->terminateStatement(stmt);     
                    return false;
                }
                strSqlite += ConvertDataType( data_type.c_str()  );
            }

            //printf("\t%s|%s|%s|%d|%s|%d\n", 
            //      tbl_name.c_str(), col_name.c_str(), data_type.c_str(), data_length ,
            //      nullable.c_str(), col_id );

            // map ==> key : table_name + col_id, value : data_type
            //mapColToType ;
            char tempBuff[100];
            sprintf(tempBuff, "%s_%d", tbl_name.c_str() , col_id );
            std::string strKey = tempBuff;

            T_DataTypeInfo tmpTypeInfo;
            tmpTypeInfo.strColName = col_name;
            tmpTypeInfo.strDataType = data_type;
            tmpTypeInfo.nNumberLength = data_length;

            std::pair<std::map<string,T_DataTypeInfo>::iterator,bool> ret;
            ret = mapColToType.insert ( std::pair<string,T_DataTypeInfo>( strKey, tmpTypeInfo ) );

        }
        stmt->closeResultSet(rs);
        conn->terminateStatement(stmt);     

        strSqlite += "\n);\n" ;
        T_QueryPerTable QueryInfo;
        QueryInfo.strTableName  = ex_tbl_name ;
        QueryInfo.strTableQuery = ex_query_str ;
        QueryInfo.nColCnt = nExColCnt ;
        vecQuery.push_back(QueryInfo);
        //printf("strSqlite [%s]\n", strSqlite.c_str() );

        char   cmdStr[256];
        // build schema script
        //remove existing db
        memset(&cmdStr, 0x00, sizeof(cmdStr) );
        snprintf(cmdStr, sizeof(cmdStr), "rm -f %s", g_str_sqlite_file.c_str() );
        system(cmdStr) ;

        FILE * outScript;
        outScript = fopen("sqlite_schema_script.sql", "w"); 
        fprintf(outScript, strSqlite.c_str() );
        fclose(outScript); 
        printf("** Creating Schema Script --> Done\n");
    }
    catch (oracle::occi::SQLException ex)
    {
        printf("exception code (%d), %s\n", ex.getErrorCode(), ex.what());
        
        if (rs) 
        {
            stmt->closeResultSet(rs);
        }

        if (stmt) 
        {
            conn->terminateStatement(stmt);
        }

        if (conn) 
        {
            env->terminateConnection(conn);
        }

        if (env) 
        {
            Environment::terminateEnvironment(env);
        }

        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////
bool ExportOracleHandler::FillSqlite() 
{
    //sqlite table is created after first open, re-open it
	char    *szErrMsg = NULL; 
	int rst = sqlite3_open( g_str_sqlite_file.c_str() , &pSQLite3);

	if ( rst )
	{
		cout<< "Can't open database: "<< sqlite3_errmsg( pSQLite3 ) <<"\n" ;
		sqlite3_close( pSQLite3 );
		pSQLite3 = NULL;
		return false;
	}

    cout << "\n** Creating Fill Script --> Start" << endl;

    char cmdStr[256];
    memset(&cmdStr, 0x00, sizeof(cmdStr) );
    snprintf(cmdStr, sizeof(cmdStr), "rm -f sqlite_fill_script.sql" );
    system(cmdStr) ;


	sqlite3_exec(pSQLite3, "BEGIN", 0, 0, 0); //transaction

    Statement *stmt = 0;
    ResultSet *rs = 0;
    string strFillScript = "";

    vector<T_QueryPerTable>::iterator end = vecQuery.end();

    for( vector<T_QueryPerTable>::iterator itor = vecQuery.begin(); itor != end; ++itor)
    {
        T_QueryPerTable QueryInfo = *itor;
        //cout << " -- table [" << QueryInfo.strTableName << "] query [" << QueryInfo.strTableQuery 
		//<< "]" << " Col Cnt [" << QueryInfo.nColCnt << "]"<<endl;
        cout << " -- table [" << QueryInfo.strTableName << "] " << " Column Cnt [" << QueryInfo.nColCnt << "]"<<endl;
        
        try
        {       
            //stmt = conn->createStatement( strQuery );

            char tmpSql [256];
            memset(&tmpSql, 0x00, sizeof(tmpSql));
            snprintf(tmpSql, sizeof(tmpSql), "SELECT * FROM %s", QueryInfo.strTableName.c_str() );
            stmt = conn->createStatement( tmpSql );

            rs = stmt->executeQuery();

			for (int i = 1; i <= QueryInfo.nColCnt; i++)
			{
				char tempBuff[100];
				sprintf(tempBuff, "%s_%d", QueryInfo.strTableName.c_str() , i );
				std::string strKey = tempBuff;

				T_DataTypeInfo dataTypeInfo = mapColToType.find ( strKey)->second ;

				if  ( 
					    dataTypeInfo.strDataType.compare(0,9,"TIMESTAMP") ==0 ||
					    dataTypeInfo.strDataType == "NUMBER" ||
					    dataTypeInfo.strDataType == "DATE" ||
					    dataTypeInfo.strDataType == "CHAR" ||
					    dataTypeInfo.strDataType == "LONG" ||
					    dataTypeInfo.strDataType == "BINARY_DOUBLE" ||
					    dataTypeInfo.strDataType == "FLOAT" 
				    ) 
				{
					rs->setMaxColumnSize(i, 40);
				}
				else
				{
					//printf("Type [%s]\n", dataTypeInfo.strDataType.c_str() );
				}
			}

            while ( rs->next() )
            {
				//printf("---\n");
                //getDate() Return the value of a column in the current row as a Date object.
                //getDouble() Return the value of a column in the current row as a C++ double.
                //getFloat() Return the value of a column in the current row as a C++ float.
                //getInt() Return the value of a column in the current row as a C++ int.
                //getString() Return the value of a column in the current row as a string.
                //getTimestamp() Return the value of a column in the current row as a Timestamp object.
                //getUInt() Return the value of a column in the current row as a C++ unsigned int

                strFillScript += "INSERT INTO " + QueryInfo.strTableName + " ( " ;

				string str_insert_query =  "INSERT INTO " + QueryInfo.strTableName + " ( " ;

                //build cols..
                for (int i = 1; i <= QueryInfo.nColCnt; i++)
                {
                    char tempBuff[100];
                    sprintf(tempBuff, "%s_%d", QueryInfo.strTableName.c_str() , i );
                    std::string strKey = tempBuff;

                    T_DataTypeInfo dataTypeInfo = mapColToType.find ( strKey)->second ;

                    if( i == 1)
                    {
                        strFillScript += dataTypeInfo.strColName ; 
						str_insert_query += dataTypeInfo.strColName ;
                    }
                    else
                    {
                        strFillScript += ", "+dataTypeInfo.strColName ; 
						str_insert_query += ", "+dataTypeInfo.strColName ;
                    }
                }

                strFillScript += " ) VALUES ( " ;
				str_insert_query += " ) VALUES ( " ;

                for (int i = 1; i <= QueryInfo.nColCnt; i++)
                {
                    char tempBuff[100];
                    sprintf(tempBuff, "%s_%d", QueryInfo.strTableName.c_str() , i );
                    std::string strKey = tempBuff;

                    T_DataTypeInfo dataTypeInfo = mapColToType.find ( strKey)->second ;
					//printf("[%d][%s]\n", i, dataTypeInfo.strDataType.c_str() );

                    if  (
                            dataTypeInfo.strDataType == "BINARY_DOUBLE" 
                        )
                    {
                        std::cout << "BINARY_DOUBLE " <<  '\n';
					}
                    if  (
                            dataTypeInfo.strDataType == "VARCHAR2" ||
                            dataTypeInfo.strDataType == "LONG" ||
                            //dataTypeInfo.strDataType == "DATE" ||
                            //dataTypeInfo.strDataType.compare(0,9,"TIMESTAMP") ==0 ||
                            dataTypeInfo.strDataType == "CHAR" 
                        )
                    {
                        //replace ' to ''

						string strConverted = "";
						if( "" != rs->getString(i) )
						{
							//printf("Data exists\n");
							string strData = rs->getString(i);
							//printf("strData [%s]\n", strData.c_str() );
							strConverted = replaceSingleQuote( strData ) ;
						}

                        if( i == 1) //i == 1 !!!!
                        {
                            strFillScript += "'"+ strConverted +"'" ; 
							str_insert_query  += "'"+ strConverted +"'" ;
                        }
                        else
                        {
                            strFillScript += ", '"+ strConverted +"'" ; 
							str_insert_query += ", '"+ strConverted +"'" ;
                        }
                    }
					
                    else if ( dataTypeInfo.strDataType.compare(0,9,"TIMESTAMP") ==0 )
                    {
                        //std::cout << " TIMESTAMP " <<  '\n';
					    string tsStr = "";
						Timestamp ts = rs->getTimestamp(i);
                        if(!ts.isNull())
						{
							tsStr = ts.toText("yyyy-mm-dd hh:mi:ss [tzh:tzm]",0);
						}
                        if( i == 1)
                        {
                            strFillScript += "'"+tsStr+"'" ; 
							str_insert_query += "'"+tsStr+"'" ;
                        }
                        else
                        {
                            strFillScript += ", '"+tsStr+"'" ; 
							str_insert_query += ", '"+tsStr+"'" ;
                        }
                        //std::cout << "  timestamp => " << tsStr << '\n';
                    }
					
                    else if ( dataTypeInfo.strDataType == "DATE" )
                    {
                        //std::cout << " DATE " <<  '\n';
                        Date tmpData = rs->getDate(i);
                        string dateStr = tmpData.toText("yyyy-mm-dd hh:mi:ss");
                        //std::cout << "  Date => " << dateStr << '\n';
                        if( i == 1)
                        {
                            strFillScript += "'"+dateStr+"'" ; 
							str_insert_query += "'"+dateStr+"'" ;
                        }
                        else
                        {
                            strFillScript += ", '"+dateStr+"'" ; 
							str_insert_query += ", '"+dateStr+"'" ;
                        }
                    }
                    else if ( 
                                dataTypeInfo.strDataType == "NUMBER" ||
								dataTypeInfo.strDataType == "BINARY_DOUBLE" ||
                                dataTypeInfo.strDataType == "FLOAT" 
                            )
                    {
						string strConverted = "";
						if( "" != rs->getString(i) )
						{
							string strData = rs->getString(i); // get as string
							//printf("strData [%s]\n", strData.c_str() );
                            if( i == 1)
                            {
                                strFillScript += strData;
								str_insert_query += strData;
                            }
                            else
                            {
                                strFillScript += ", "+ strData;
								str_insert_query += ", "+ strData;
                            }
						}
                        else
                        {
                            //std::cout << "  Number is NULL! "  << '\n';
                            if( i == 1)
                            {
                                strFillScript += "NULL" ; 
								str_insert_query += "NULL" ;
                            }
                            else
                            {
                                strFillScript += ", NULL"; 
								str_insert_query  += ", NULL";
                            }
                        }
                        /*
                        Number tmpData = rs->getNumber(i); //get Number class
                        if(!tmpData.isNull())
                        {
                            if( i == 1)
                            {
                                strFillScript += tmpData.toText(env, "999999999999") ; 
                            }
                            else
                            {
                                strFillScript += ", "+ tmpData.toText(env, "999999999999"); 
                            }
                        }
                        else
                        {
                            //std::cout << "  Number is NULL! "  << '\n';
                            if( i == 1)
                            {
                                strFillScript += "NULL" ; 
                            }
                            else
                            {
                                strFillScript += ", NULL"; 
                            }
                        }
						*/
                    }
					else
					{
						printf("ERROR else!!\n");
					}
                } //for

                strFillScript += " );\n" ;
				str_insert_query += " )" ;

                //std::cout << "  Fill => " << str_insert_query << '\n'; //debug

                //sqlite api 
				int rslt = sqlite3_exec(pSQLite3, str_insert_query.c_str(), NULL, 0, &szErrMsg);

				if (rslt != SQLITE_OK)
				{
					printf ("Sqlite Error [%s]\n", szErrMsg ); 
					sqlite3_free( szErrMsg );
					sqlite3_close( pSQLite3);
					sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction

					if (rs) 
					{
						stmt->closeResultSet(rs);
					}

					if (stmt) 
					{
						conn->terminateStatement(stmt);
					}

					if (conn) 
					{
						env->terminateConnection(conn);
					}

					if (env) 
					{
						Environment::terminateEnvironment(env);
					}
					return false;
				}
            } //while

        }
        catch (oracle::occi::SQLException ex)
        {
            printf("exception code (%d), %s\n", ex.getErrorCode(), ex.what());
            
            if (rs) 
            {
                stmt->closeResultSet(rs);
            }

            if (stmt) 
            {
                conn->terminateStatement(stmt);
            }

            if (conn) 
            {
                env->terminateConnection(conn);
            }

            if (env) 
            {
                Environment::terminateEnvironment(env);
            }

			sqlite3_free( szErrMsg );
			sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction

            return false;
        }
    } //for
            
    stmt->closeResultSet(rs);
    conn->terminateStatement(stmt);     

	sqlite3_free( szErrMsg );
	sqlite3_exec(pSQLite3, "END", 0, 0, 0); //transaction

    FILE * outScript;
    outScript = fopen("sqlite_fill_script.sql", "w"); 
    fprintf(outScript, strFillScript.c_str() );

    fclose(outScript); 

    cout << "** Creating Fill Script --> Done" << endl;

    return true;
}

////////////////////////////////////////////////////////////////////////////////
void ExportOracleHandler::Terminate()
{
	sqlite3_close( pSQLite3);

    env->terminateConnection(conn);     
    Environment::terminateEnvironment(env);
}

