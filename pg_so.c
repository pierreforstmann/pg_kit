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

static int do_system(char *command)
{
   int rc;

   rc = system(command);
   printf("system rc=%d \n", rc);
}

static int do_exec(PGconn *conn, char *stmt)
{
    PGresult   *res;
    int         nFields;
    int         i;
    int         j;
    char        *result;

    /*
     * EXECUTE
     */

    res = PQexec(conn, stmt);

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

     /*
     * free resources before exit
     */

    PQclear(res);

    return 0;

}

static int do_exec00(PGconn *conn, char *query)
{
    PGresult   *res;
    int		nFields;
    int		i;
    int		j;
    char	*result;

    /*
     * EXECUTE
     */

    res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "PQexec failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

     /*
     * free resources before exit
     */

    PQclear(res); 

    return 0;
}


static char* do_exec11(PGconn *conn, char *query, char *param)
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

    /*
     * EXECUTE
     */

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

static int do_exec12(PGconn *conn, char *query, char *param, char **param_out_1, char **param_out_2)
{
    PGresult   *res;
    const char *paramValues[2];
    int         paramLengths[2];
    int         paramFormats[2];
    uint32_t    binaryIntVal;
    int		nFields;
    int		i;
    int		j;

    paramValues[0] = param; 

    /*
     * EXECUTE
     */

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
		   strcpy(*param_out_1, PQgetvalue(res, i, j)); 
	    if (i == 0 && j == 1)
		   strcpy(*param_out_2, PQgetvalue(res, i, j)); 
	}
        printf("\n");
    }

    if (PQntuples(res) == 0)
	    return 1;

    /*
     * free resources before exit
     */
    PQclear(res);

    return 0;
}
int
main(int argc, char **argv)
{
    const char *conninfo;
    PGconn     *conn;
    char dd_param_name[MAX_LENGTH];
    char *dd_param_value;
    char *ru_param_value;
    char *ca_param_value;
    char be_param_name[MAX_LENGTH];
    char command[MAX_LENGTH];
    int rc;


    conn = do_connect(argc, argv);

    strcpy(dd_param_name, "data_directory"); 
  
    dd_param_value = do_exec11(conn,  
                              "SELECT setting FROM pg_settings WHERE name=$1",
			      dd_param_name);

    printf("dd_param_value=%s\n", dd_param_value);

    ru_param_value = (char *)malloc(MAX_LENGTH);
    if (ru_param_value == NULL)
    {
	fprintf(stderr, "malloc %d failed \n");
	exit_nicely(conn);
    }

    ca_param_value = (char *)malloc(MAX_LENGTH);
    if (ca_param_value == NULL)
    {
	fprintf(stderr, "malloc %d failed \n");
	exit_nicely(conn);
    }


    strcpy(be_param_name, "walsender");
    rc = do_exec12(conn,
                  "SELECT usename, client_addr FROM pg_stat_activity WHERE backend_type=$1",
                  be_param_name, &ru_param_value, &ca_param_value);

    if (rc == 1)
    {	
	    fprintf(stderr, "ERROR: Cannot find standby \n");
	    exit_nicely(conn);
    }    

    printf("ru_param_value=%s\n", ru_param_value);
    printf("ca_param_value=%s\n", ca_param_value);

    do_exec00(conn, "SELECT pg_switch_wal();");

    do_exec(conn, "checkpoint;");

    command[0]='\0'; 
    strcpy(command, "ALTER SYSTEM SET primary_conninfo='host=");
    strcat(command, ca_param_value);
    strcat(command, " user=");
    strcat(command, ru_param_value);
    strcat(command, "'");
    do_exec(conn, command);

    do_system("pg_ctl stop");

    command[0]='\0';
    strcpy(command, "touch "); 
    strcat(command, dd_param_value);
    strcat(command, "/standby.signal");
    do_system(command);

    do_system("pg_ctl start");

    do_disconnect(conn);

    return 0;
}
