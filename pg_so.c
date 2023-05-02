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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <getopt.h>
#include "libpq-fe.h"

#define MAX_LENGTH 128

int verbose;

static void usage(void)
{
	printf(("pg_so \n\n"));
	printf(("Usage:\n"));
	printf(("  pg_so [OPTION]...\n\n"));
	printf(("Options:\n"));
	printf(("  -p, --port \n"));
	printf(("  -v, --verbose \n"));
	printf(("\n"));
}


static void abort_nicely()
{
    exit(1);
}

char *build_string0()
{
	char *output;
        
        output = (char *)malloc(MAX_LENGTH + 1); 
        if (output == NULL)
        {
            fprintf(stderr, "build_string: malloc failed for size=%d\n", MAX_LENGTH + 1);
            abort_nicely();
        }

	return output;
}

char *build_string(char *format, char *p1, char *p2, char *p3)
{
    int size;
    char *output;
    
    if (p1 == NULL && p2 == NULL && p3  == NULL)
	size = snprintf(0, 0, format);
    else if (p2 == NULL && p3 == NULL)
        size = snprintf(0, 0, format, p1);
    else if (p3 == NULL)
        size = snprintf(0, 0, format, p1, p2);
    else 
        size = snprintf(0, 0, format, p1, p2, p3);

    output = (char *)malloc(size + 1); 
    if (output == NULL)
    {
        fprintf(stderr, "build_string: malloc failed for %s=\n", format);
        abort_nicely();
    }

    if (p1 == NULL && p2 == NULL && p3 == NULL)
        snprintf(output, size + 1, format);
    if (p2 == NULL && p3 == NULL)
        snprintf(output, size + 1, format, p1);
    else if (p3 == NULL)
        snprintf(output, size + 1, format, p1, p2);
    else
        snprintf(output, size + 1, format, p1, p2, p3);

    return output;

}

static void exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

static PGconn* do_local_connect()
{
    const char *conninfo;
    PGconn     *conn;

    conninfo = "dbname = postgres";

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "%s \n", PQerrorMessage(conn));
	exit_nicely(conn);
    }

    return conn;
}

static PGconn* do_remote_connect(char *hostname, char *port)
{
    char *conninfo;
    int  size;
    PGconn     *conn;

    conninfo  = build_string("postgresql://postgres@%s:%s/postgres", hostname, port, NULL);

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "%s \n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

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
   if (rc != 0)
   {
	fprintf(stderr, "%s failed - return code=%d\n", command, rc);
        abort_nicely();
   }
   
   return rc;
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
        fprintf(stderr, "PQexec failed: %s\n", PQerrorMessage(conn));
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
        fprintf(stderr, "PQexec failed: %s \n", PQerrorMessage(conn));
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
        fprintf(stderr, "PQexecParams failed: %s \n", PQerrorMessage(conn));
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
    nFields = PQnfields(res);
    for (i = 0; i < PQntuples(res); i++)
    {
        for (j = 0; j < nFields; j++)
	{
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

    nFields = PQnfields(res);
    for (i = 0; i < PQntuples(res); i++)
    {
        for (j = 0; j < nFields; j++)
	{
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
    PGconn     *conn_p;
    PGconn     *conn_s;
    char *dd_param_name;
    char *dd_param_value;
    char *ru_param_value;
    char *ca_param_value;
    char *be_param_name = "walsender";
    char *command;
    char port_s[MAX_LENGTH] = "";
    int rc;

    static struct option long_options[] = 
    {
		{"port", required_argument, NULL, 'p'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
    };

    int c;
    int optindex = 0;

    verbose = 0;

    if (argc > 1)
    {
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
    }

    while (( c = getopt_long(argc, argv, "p:v", long_options, &optindex)) != -1)
    {
		switch (c)
		{
	            case 'p':
			        strcpy(port_s, optarg);
                                break;
                    case 'v':
				verbose = 1;
				break;
		    default:
				fprintf(stderr, "Try \"pg_so --help\" for more information.\n");
				abort_nicely();
				break;
		}
    }


    if (strlen(port_s) == 0)
	strcpy(port_s, "5432");

    /*
     * PRIMARY
     */

    conn_p = do_local_connect();

    dd_param_name = build_string("data_directory", NULL, NULL, NULL);
  
    if (verbose)
	    printf("get PGDATA from local primary");
    dd_param_value = do_exec11(conn_p,  
                              "SELECT setting FROM pg_settings WHERE name=$1",
			      dd_param_name);
    if (verbose)
	    printf("local PGDATA=%s\n...done.", dd_param_value);

    ru_param_value = build_string0();
    ca_param_value = build_string0();

    rc = do_exec12(conn_p,
                  "SELECT usename, client_addr FROM pg_stat_activity WHERE backend_type=$1",
                  be_param_name, &ru_param_value, &ca_param_value);

    if (rc == 1)
    {	
	    fprintf(stderr, "ERROR: Cannot find standby \n");
	    exit_nicely(conn_p);
    }    


    if (verbose)
	    printf("switch WAL on local primary ...\n");
    do_exec00(conn_p, "SELECT pg_switch_wal();");
    if (verbose)
	    printf("... done.\n");

    if (verbose)
	    printf("checkpoint on local primary ...\n");
    do_exec(conn_p, "checkpoint;");
    if (verbose)
	    printf("... done.\n");

    if (verbose)
	    printf("set primary_conninfo on local primary ...\n");
    command = build_string("ALTER SYSTEM SET primary_conninfo='host=%s port=%s user=%s'", ca_param_value, port_s, ru_param_value);
    do_exec(conn_p, command);
    if (verbose)
	    printf("... done.\n");
    do_disconnect(conn_p);

    if (verbose)
	    printf("stop local primary ...\n");
    do_system("pg_ctl stop");
    if (verbose)
	    printf("... done.\n");

    if (verbose)
	    printf("create local standby.signal ...\n");
    command = build_string("touch %s/standby.signal", dd_param_value, NULL, NULL);
    do_system(command);
    if (verbose)
	    printf("... done.\n");

    if (verbose)
	    printf("restart old primary as new standby...\n");
    do_system("pg_ctl start");
    if (verbose)
	    printf("... done.\n");


    /*
     * STANDBY
     */

    conn_s = do_remote_connect(ca_param_value, port_s);
    if (verbose)
	    printf("promote old standby as new primary...\n");
    do_exec00(conn_s, "SELECT pg_promote();");
    if (verbose)
	    printf("... done.\n");

    return 0;
}
