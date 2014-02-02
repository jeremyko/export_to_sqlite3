
#ifndef _EXPORT_HANDLER_INTERFACE_HPP_
#define _EXPORT_HANDLER_INTERFACE_HPP_

#include "common.hpp"

class ExportHandlerInterface
{
public:
    ExportHandlerInterface(){};
    ExportHandlerInterface( string& user, 
                            string& pass_wd, 
                            string& conn_str, 
							string& ip, 
							int port, 
							string& db_name );

    virtual ~ExportHandlerInterface() {};

public:
    virtual bool ConnectDB() = 0;
    virtual bool BuildSchemaScript() = 0;
    virtual bool FillSqlite() = 0;
	virtual bool GetFtpInfo( VEC_FTP_INFO & ) =0;
    virtual void Terminate() = 0;
};
#endif
