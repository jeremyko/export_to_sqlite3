
#oracle
INC_ORACLE_PATH  = $(ORACLE_HOME)/precomp/public
LIBHOME64=$(ORACLE_HOME)/lib
PRODLIBHOME64=$(ORACLE_HOME)/precomp/lib
ORA_LFLAGS=-L$(PRODLIBHOME64) -L$(LIBHOME64)
INC_OCCI_PATH = $(ORACLE_HOME)/rdbms/public
LIB_ORACLE_PATH  = $(ORACLE_HOME)/lib -lclntsh -locci

#mysql
INC_MYSQL_PATH   = /MYSQL/mysql_5.5.15/include
LIB_MYSQL_PATH   =/MYSQL/mysql_5.5.15/lib -lmysqlclient_r

#sqlite3
LIB_SQLITE_SO = -lsqlite3 

INC_LOCAL_PATH          = .
INC_LOCAL_INC_PATH      = ../INC
INC_LOCAL_SRC_PATH      = ../SRC
LIB_SYS_PATH     = /usr/lib64
LIB_THREAD_SO = -lpthread
LIB_CURSES_SO = -lcurses

# Compiler Define
#####################################################################
CC         = g++
MAKE       = make
######################################################################

CFLAGS = -Wall -Wno-write-strings -O0 -g -D_LINUX_ -D_LITTLE_ENDIAN_ -D__LINUX $(LOCALFLAGS)

LFLAGS = -L$(LIB_SYS_PATH)  -L$(LIB_ORACLE_PATH) -L$(LIB_MYSQL_PATH) \
            $(LIB_ORACLE_SO) $(LIB_OCCI_SO) $(ORA_LFLAGS) $(LIB_THREAD_SO) $(LIB_CURSES_SO) $(LIB_SQLITE_SO)

IFLAGS = -I$(INC_LOCAL_PATH) -I$(INC_MYSQL_PATH)\
         -I$(INC_ORACLE_PATH) -I$(INC_LOCAL_INC_PATH) -I$(INC_LOCAL_SRC_PATH) -I$(INC_OCCI_PATH)

.SUFFIXES:.cpp .c .o

.cpp.o:
	$(CC) -c -o $*.o $(CFLAGS) $(IFLAGS) $*.cpp

.c.o:
	$(CC) -c -o $*.o $(CFLAGS) $(IFLAGS) $*.c
                                       
######################################################
EXE =  export_to_sqlite

OBJS = common.o export_to_sqlite.o export_oracle_handler.o export_mysql_handler.o

all : $(EXE)

$(EXE) : $(OBJS)
	$(CC) -o $@ $(OBJS) $(PROLDLIBS) $(INC_PATH) $(COMM_LIB) $(LFLAGS)

install : $(EXE)
	cp $(EXE) $(BIN_PATH)      
	rm -f $(EXE) *.o *.lis core 

clean : 
	rm -f $(EXE) *.o *.lis core 
