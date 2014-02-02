#ifndef _COMMON_HPP_
#define _COMMON_HPP_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <map>
#include <iostream>
//#include <locale.h>

using namespace std;

typedef struct _QueryPerTable
{
    string strTableName;
    string strTableQuery;
    int nColCnt;
} T_QueryPerTable;

typedef struct _DataTypeInfo
{
    string strColName;
    string strDataType;
    int nNumberLength;
} T_DataTypeInfo;


typedef enum _ENUM_DB_TYPE 
{
    DB_TYPE_ORACLE,
    DB_TYPE_MYSQL,
    DB_TYPE_SQLITE,
    MAX_DB_TYPE
    
} ENUM_DB_TYPE;

typedef struct _T_DATA_TYPE_MAP
{
    char    szDataTypeFrom [20];
    char    szDataTypeTo   [20];
} T_DATA_TYPE_MAP;


///////////////////////////////////////////////////////////////////////////////
string replaceSingleQuote( string const& original );

#endif
