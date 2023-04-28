/*
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * 
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Portions Copyright(c) 2023 Pierre Forstmann
 *
 * pg_so 
 *
 *
 */

#ifdef WIN32
#include <windows.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include "libpq-fe.h"

#define MAX_LENGTH 128

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

static PGconn* do_connect(int argc, char **argv)
{
    const char *conninfo;
    PGconn     *conn;
    PGresult   *res;

    /*
     * If the user supplies a parameter on the command line, use it as the
     * conninfo string; otherwise default to setting dbname=postgres and using
     * environment variables or defaults for all other connection parameters.
     */
    if (argc > 1)
        conninfo = argv[1];
    else
        conninfo = "dbname = postgres";

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "%s", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    /* Set always-secure search path, so malicious users can't take control. */
    res = PQexec(conn, "SET search_path = test1");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    return conn;
}

static void do_disconnect(PGconn *conn)
{
    /* close the connection to the database and cleanup */
    PQfinish(conn);
}

static char* do_exec1(PGconn *conn, char *query, char *param)
{
    PGresult   *res;
    const char *paramValues[2];
    int         paramLengths[2];
    int         paramFormats[2];
    uint32_t    binaryIntVal;
    int		nFields;
    int		i;
    int		j;
    char	*result;

    paramValues[0] = param; 

    res = PQexecParams(conn,
		       query,
                       1,       /* number of parameters */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */


    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexecParams failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    result = (char *)malloc(MAX_LENGTH);
    if (result == NULL)
    {
	fprintf(stderr, "malloc %d failed \n");
	exit_nicely(conn);
    }

    /*
     * FETCH: retrieve results
     */

    /* first, print out the attribute names */
    nFields = PQnfields(res);
    for (i = 0; i < nFields; i++)
        printf("%-15s", PQfname(res, i));
    printf("\n\n");

    /* next, print out the rows */
    for (i = 0; i < PQntuples(res); i++)
    {
        for (j = 0; j < nFields; j++)
	{
            printf("%-15s", PQgetvalue(res, i, j));
	    if (i == 0 && j == 0)
		   strcpy(result, PQgetvalue(res, i, j)); 
	}
        printf("\n");
    }


    /*
     * free resources before exit
     */
    PQclear(res);

    return result;
}

int
main(int argc, char **argv)
{
    const char *conninfo;
    PGconn     *conn;
    char dd_param_name[MAX_LENGTH];
    char *dd_param_value;

    /*
     * EXECUTE
     */

    conn = do_connect(argc, argv);
    strcpy(dd_param_name, "data_directory"); 
  
    dd_param_value = do_exec1(conn,  
                              "SELECT setting FROM pg_settings WHERE name=$1",
			      dd_param_name);

    printf("dd_param_value=%s\n", dd_param_value);
    do_disconnect(conn);

    return 0;
}
