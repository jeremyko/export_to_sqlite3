#ifndef _EXPORT_HANDLER_ORACLE_HPP_
#define _EXPORT_HANDLER_ORACLE_HPP_

#include "common.hpp"
#include "export_handler_interface.hpp"
#include <sqlite3.h>
#include <occi.h>

using namespace oracle::occi;

class ExportOracleHandler: public ExportHandlerInterface
{
public:
    ExportOracleHandler( string&  _user, 
                        string&  _pass_wd, 
						string&  _ip, 
						int  _port, 
						string&  _db_name );

    ~ExportOracleHandler();

public:
    bool ConnectDB() ;
    bool BuildSchemaScript() ;
    bool FillSqlite() ;
    void Terminate() ;

private:
	char* ConvertDataType( const char* szFrom );
    Environment *env ;
    Connection *conn ;

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
