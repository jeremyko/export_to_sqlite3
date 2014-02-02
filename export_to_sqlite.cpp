#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <locale.h>
#include <algorithm>  

#include "export_handler_interface.hpp"
#include "export_oracle_handler.hpp"
#include "export_mysql_handler.hpp"

using namespace std;

string g_str_sqlite_file = "";

ExportHandlerInterface* g_export_handler = NULL;

///////////////////////////////////////////////////////////////////////////////
bool RunSchemaScript ()
{
	printf("\n** Creating DB --> Start\n");
    char cmdStr[256];
	memset(&cmdStr, 0x00, sizeof(cmdStr) );
	snprintf(cmdStr, sizeof(cmdStr), "time sqlite3 %s < sqlite_schema_script.sql", g_str_sqlite_file.c_str() );

	if( system(cmdStr) < 0 )
	{
		printf("[%s-%d] error calling script [%s] err[%d]\n", __FILE__,__LINE__,cmdStr, errno);
		return false;
	}

	printf("** Creating DB --> Done\n");
    return true;
}

///////////////////////////////////////////////////////////////////////////////
/*  --> because this is very slow. sqlite api call is chosen.
bool RunFillScript ()
{
	cout << "\n** RunFillScript --> started, please wait " << endl;
    char cmdStr[256];
	memset(&cmdStr, 0x00, sizeof(cmdStr) );

	snprintf(cmdStr, sizeof(cmdStr), "time sqlite3 %s < sqlite_fill_script.sql", g_str_sqlite_file.c_str() );

	if( system(cmdStr) < 0 )
	{
		printf("[%s-%d] error run script [%s] err[%d]\n", __FILE__,__LINE__,cmdStr, errno);
		return false;
	}

	cout << "** RunFillScript --> Done " << endl;
    return true;
    return true;
}
*/


///////////////////////////////////////////////////////////////////////////////


void show_usage ()
{
	printf("\n\nusage : export_to_sqlite SQLITE_FILE  DB_TYPE  DB_USER  DB_PASSWD  IP PORT DB_NAME\n");
	printf("   ex : export_to_sqlite ofcs.sqlite ORACLE OFCS OFCS 192.168.1.225 1521 orcl\n");
	printf("   ex : export_to_sqlite ofcs.sqlite MYSQL  pcn  pcn  192.168.1.204 3306 PCN\n");
	//printf("   export_to_sqlite help -> show this message\n\n");
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
	string str_db_type     = "";
	string str_db_user     = "";
	string str_db_passwd   = "";
	string str_db_ip_addr  = ""; 
	int n_db_port     = 0;
	string str_db_name     = "";

	if( argc < 8 || strcmp(argv[1], "help")==0 )
	{
	    show_usage();
		return -1;
	}

    g_str_sqlite_file  =   argv[1] ; //sqlite file name 
    str_db_type        =   argv[2] ; //db type : ORACLE/MYSQL
    str_db_user        =   argv[3] ; //db user
    str_db_passwd      =   argv[4] ; //db password
    str_db_ip_addr     =   argv[5] ; //db ip address
    n_db_port          =   atoi(argv[6] ); //db  port
    str_db_name        =   argv[7] ; //db name
    
    cout << str_db_type << endl;


	if( str_db_type == "ORACLE")
	{
		g_export_handler = new ExportOracleHandler( str_db_user, str_db_passwd, str_db_ip_addr, n_db_port, str_db_name );
	}
	else if( str_db_type == "MYSQL")
	{
	    g_export_handler = new ExportMysqlHandler(str_db_user, str_db_passwd, str_db_ip_addr, n_db_port, str_db_name);	
	}
	else
	{
	    show_usage();
		return -1;
	}


	//connect db
	if ( ! g_export_handler->ConnectDB() )
	{	
		printf("Failed to Connect to Database!!\n");
		return -1;
	}

	//build and run schema script
	bool bRslt = false;
	bRslt = g_export_handler->BuildSchemaScript ();
	if(!bRslt)
	{
		printf("[%s-%d] error BuildSchemaScript\n", __FILE__,__LINE__);
		return -1;
	}

	bRslt = RunSchemaScript ();
	if(!bRslt)
	{
		printf("[%s-%d] error RunSchemaScript\n", __FILE__,__LINE__);
		return -1;
	}

	//fill sqlite db
	bRslt = g_export_handler->FillSqlite ();
	if(!bRslt)
	{
		printf("[%s-%d] error FillSqlite\n", __FILE__,__LINE__);
		return -1;
	}

	g_export_handler->Terminate(); 

    cout << "\n** All Done "<< endl;

    return 0;
}

