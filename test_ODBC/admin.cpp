#include <windows.h>
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <string>
#include <sstream>
#include <ctime>

#pragma warning(disable: 4996)

using std::string;
using std::stringstream;

struct record
{
    unsigned int id;
    const char* name;
    const char* description;
};

class DBManager
{
public:
    DBManager(const string& inDSN,
              const string& inUserName,
              const string& inPassword)
    :
        mConHandle(NULL)
      , mEnvHandle(NULL)
      , mStmtHandle(NULL)
      , mDSN(inDSN)
      , mUserName(inUserName)
      , mPassword(inPassword)
    {
    }

    ~DBManager()
    {
        if (mStmtHandle)
        {
            SQLFreeHandle(SQL_HANDLE_STMT, mStmtHandle);
            mStmtHandle = NULL;
        }

        if (mConHandle)
        {
            SQLDisconnect(mConHandle);
            SQLFreeHandle(SQL_HANDLE_DBC, mConHandle);
            mConHandle = NULL;
        }

        if (mEnvHandle) {
            SQLFreeHandle(SQL_HANDLE_ENV, mEnvHandle);
            mEnvHandle = NULL;
        }
    }

    int open()
    {
        if (NULL == mEnvHandle)
        {
            SQLRETURN ret;
            ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &mEnvHandle);

            if (SQL_SUCCESS == ret)
            {
                ret = SQLSetEnvAttr(mEnvHandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);

                if (SQL_SUCCESS == ret)
                {
                    ret = SQLAllocHandle(SQL_HANDLE_DBC, mEnvHandle, &mConHandle);
                    if (SQL_SUCCESS != ret)
                    {
                        return ret;
                    }
                }
                else return ret;
            }
            else {
                return ret;
            }
        }
        else {
            //@todo
        }

        return 0;
    }

    int connect()
    {
        if (mConHandle)
        {
            SQLRETURN ret;
            ret = SQLConnect(mConHandle
                , (SQLCHAR *)mDSN.c_str(), SQL_NTS
                , (SQLCHAR *)mUserName.c_str(), SQL_NTS
                , (SQLCHAR *)mPassword.c_str(), SQL_NTS);

            if (SQL_SUCCESS == ret)
            {
                return SQLAllocHandle(SQL_HANDLE_STMT, mConHandle, &mStmtHandle);
            }
            else return ret;
        }
        else return -1;
    }

    void print_driver_info()
    {
        SQLCHAR dbms_name[256], dbms_ver[256];
        SQLUINTEGER getdata_support;
        SQLUSMALLINT max_concur_act;

        printf("Connected\n");
        /*
        *  Find something out about the driver.
        */
        SQLGetInfo(mConHandle, SQL_DBMS_NAME, (SQLPOINTER)dbms_name, sizeof(dbms_name), NULL);
        SQLGetInfo(mConHandle, SQL_DBMS_VER, (SQLPOINTER)dbms_ver, sizeof(dbms_ver), NULL);
        SQLGetInfo(mConHandle, SQL_GETDATA_EXTENSIONS, (SQLPOINTER)&getdata_support, 0, 0);
        SQLGetInfo(mConHandle, SQL_MAX_CONCURRENT_ACTIVITIES, &max_concur_act, 0, 0);

        printf("DBMS Name: %s\n", dbms_name);
        printf("DBMS Version: %s\n", dbms_ver);
        if (max_concur_act == 0) {
            printf("SQL_MAX_CONCURRENT_ACTIVITIES - no limit or undefined\n");
        }
        else {
            printf("SQL_MAX_CONCURRENT_ACTIVITIES = %u\n", max_concur_act);
        }
        if (getdata_support & SQL_GD_ANY_ORDER)
            printf("SQLGetData - columns can be retrieved in any order\n");
        else
            printf("SQLGetData - columns must be retrieved in order\n");
        if (getdata_support & SQL_GD_ANY_COLUMN)
            printf("SQLGetData - can retrieve columns before last bound one\n");
        else
            printf("SQLGetData - columns must be retrieved after last bound one\n");
    }

    void print_results()
    {
        SQLSMALLINT columns;
        SQLRETURN ret;
        int row(0);

        /* Retrieve a list of tables */
        SQLTables(mStmtHandle, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"TABLE", SQL_NTS);
        /* How many columns are there */
        SQLNumResultCols(mStmtHandle, &columns);
        /* Loop through the rows in the result-set */
        while (SQL_SUCCEEDED(ret = SQLFetch(mStmtHandle))) {
            SQLUSMALLINT i;
            printf("Row %d\n", row++);
            /* Loop through the columns */
            for (i = 1; i <= columns; i++) {
                SQLLEN indicator;
                char buf[512];
                /* retrieve column data as a string */
                ret = SQLGetData(mStmtHandle, i, SQL_C_CHAR, buf, sizeof(buf), &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    /* Handle null columns */
                    if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
                    printf("  Column %u : %s\n", i, buf);
                }
            }
        }
    }

    void _execSQL(const string& sqlstr)
    {
        SQLCHAR SQLStmt[256] = { 0 };

        SQLPrepare(mStmtHandle, (SQLCHAR*)sqlstr.c_str(), SQL_NTS);
        SQLExecute(mStmtHandle);

        //SQLRETURN ret;

        //strcpy((char *)SQLStmt, sqlstr.c_str());

        //ret = SQLExecDirect(mStmtHandle, SQLStmt, SQL_NTS);
        //if (SQL_SUCCESS == ret)
        //{
        //    if (SQL_SUCCESS == SQLFetch(mStmtHandle))
        //    {
        //        //printf("Exec SQL \"%s\" succesfully!\n", sqlstr.c_str());
        //    }
        //}
    }

    void insertRecord(const struct record& r);

private:
    SQLHDBC     mConHandle;
    SQLHENV     mEnvHandle;
    SQLHSTMT    mStmtHandle;
    string      mDSN;
    string      mUserName;
    string      mPassword;
};


inline void
DBManager::insertRecord(const struct record& r)
{
    stringstream ss;
    ss << "INSERT into system.TEST_TABLE1 VALUES(" << r.id << ",'" << r.name << "'," << "'" << r.description << "');";
    this->_execSQL(ss.str());
}

int main()
{
    //DBManager dbManager("Local_Hana_DB", "system", "Aa314520");
    //DBManager dbManager("Local_MySQL_DB", "system", "1qaz@WSX3edc");
    //DBManager dbManager("Remote_MySQL_DB", "system", "1qaz@WSX3edc");
    DBManager dbManager("Remote_HANA_DB", "system", "1qaz@WSX3edc"); // remote hana server

    if (0 == dbManager.open())
    {
        if (0 == dbManager.connect())
        {
            std::clock_t start;
            double duration;
            start = std::clock();

            int count(100);
            for (int i = 1; i <= count; ++i)
            {
                struct record r;
                r.id = i;
                r.name = "test";
                r.description = "YES!";
                dbManager.insertRecord(r);
            }
            //stringstream ss;
            //ss << "DELETE from test.TEST_TABLE1 where id < " << count << ";";
            //dbManager.testSQL(ss.str());

            duration = (std::clock() - start) / (double)CLOCKS_PER_SEC;
            /*printf("After delete SQL to local MySQL DB, duration: %lf \n", duration);*/

            printf("After %d insert SQL to local HANA DB, duration: %lf \n", count, duration);
        }
    }

    system("pause");
    // Return To The Operating System
    return 0;
}
