
#ifndef _EXPORT_HANDLER_MYSQL_HPP_
#define _EXPORT_HANDLER_MYSQL_HPP_

#include "common.hpp"
#include "export_handler_interface.hpp"
#include <mysql.h>
#include <my_global.h>
#include <sqlite3.h>

class ExportMysqlHandler: public ExportHandlerInterface
{
public:
    ExportMysqlHandler( string&  _user, 
                        string&  _pass_wd, 
						string&  _ip, 
						int  _port, 
						string&  _db_name );
    //ExportMysqlHandler();
    ~ExportMysqlHandler();

public:
    bool ConnectDB() ;
    bool BuildSchemaScript() ;
    bool FillSqlite() ;
    void Terminate() ;

private:
	char* ConvertDataType( const char* szFrom );

    char errmsg_ [1024];
    MYSQL* mysql_handler_ ;   

    string user_ ;
    string pass_wd_  ;
    string ip_ ;
    int port_ ;
    string db_name_ ;

	vector <T_QueryPerTable> vecQuery ;
	map <string, T_DataTypeInfo> mapColToType ;

    sqlite3 *pSQLite3 ; 
};
#endif
