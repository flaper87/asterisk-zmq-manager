/*
 * res_zmq_manager.c
 *
 * Copyright 2011 Flavio [FlaPer87] Percoco Premoli <flaper87@flaper87.org>
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License v2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*! \file
 *
 * \brief 0MQ Manager
 *
 * \author Flavio [FlaPer87] Percoco Premoli <flaper87@flaper87.org>
 *
 */

#include <asterisk.h>

ASTERISK_FILE_VERSION(__FILE__, "$Revision: $")

#include <time.h>
#include <errno.h>
#include <regex.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h> 
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

// #include <asterisk/config.h>
// #include <asterisk/options.h>
#include <asterisk/channel.h>
// #include <asterisk/manager.h>
#include <asterisk/cli.h>
#include <asterisk/module.h>
#include <asterisk/version.h>
// #include <asterisk/logger.h>
// #include <asterisk/strings.h>
// #include <asterisk/linkedlists.h>
// #include <asterisk/threadstorage.h>
// #include <asterisk/file.h>
// #include <asterisk/callerid.h>
// #include <asterisk/lock.h>
// #include <asterisk/app.h>
// #include <asterisk/pbx.h>
// #include <asterisk/md5.h>
// #include <asterisk/acl.h>
// #include <asterisk/utils.h>
// #include <asterisk/ast_version.h>
// #include <asterisk/linkedlists.h>
// #include <asterisk/term.h>
// #include <asterisk/astobj2.h>
// #include <asterisk/features.h>
// #include <asterisk/security_events.h>
// #include <asterisk/aoc.h>

#include <zmq.h>
#include <jansson.h>

#define DEBUG(fmt, args...) ast_debug(1, "[0MQ Manager Debug]: "fmt, args);
#define ERROR(fmt, args...) ast_log(LOG_ERROR, "[0MQ Manager Error]: "fmt, args);


static char *name = "zmq_manager";
static char *config = "zmq_manager.conf";
static char *desc = "Manager based on zmq message";

/*
 * CLI VARS
 */
static int calls = 0;
static int binded = 0;


// Number of workers running
static int cur_workers = 0;

// Workers counter mutex
static ast_mutex_t workers_mutex;
static void ast_zmq_thread(void *data);

struct unload_string {
    AST_LIST_ENTRY(unload_string) entry;
    struct ast_str *str;
};

struct ast_zmq_pthread_data {
    int max_workers;
    struct ast_str *connection_string;
    pthread_t master;
    int accept_fd;
    void *(*fn)(void *);
    const char *name;
};

static struct ast_zmq_pthread_data zmq_data = {
    .master = AST_PTHREADT_NULL,
    .fn = (void *)ast_zmq_thread,
    .name = "ZMQ Thread",
    .accept_fd = -1,
    .max_workers = 0
};

static AST_LIST_HEAD_STATIC(unload_strings, unload_string);

static int 
load_config_number(struct ast_config *cfg, const char *category, const char *variable, int *field, int def)
{
    const char *tmp;

    tmp = ast_variable_retrieve(cfg, category, variable);

    if (!tmp || sscanf(tmp, "%d", field) < 1)
        *field = def;

    return 0;
}

static int 
load_config_string(struct ast_config *cfg, const char *category, const char *variable, struct ast_str **field, const char *def)
{
    struct unload_string *us;
    const char *tmp;

    if (!(us = ast_calloc(1, sizeof(*us))))
        return -1;

    if (!(*field = ast_str_create(16))) {
        ast_free(us);
        return -1;
    }

    tmp = ast_variable_retrieve(cfg, category, variable);

    ast_str_set(field, 0, "%s", tmp ? tmp : def);

    us->str = *field;

    AST_LIST_LOCK(&unload_strings);
    AST_LIST_INSERT_HEAD(&unload_strings, us, entry);
    AST_LIST_UNLOCK(&unload_strings);

    return 0;
}

/*
 * CLI command handler.
 * 
 * Shows whether the socket is binded or not and the number of calls made so far.
 */
static char *
handle_cli_zmq_manager_status(struct ast_cli_entry *e, int cmd, struct ast_cli_args *a)
{
 switch (cmd) {
 case CLI_INIT:
     e->command = "zmq manager status";
     e->usage =
         "Usage: zmq manager status\n"
         "       Shows useful stats about zmq manager usage\n";
     return NULL;
 case CLI_GENERATE:
     return NULL;
 }

 if (a->argc != 3)
     return CLI_SHOWUSAGE;
 
 if (binded!=0)
     ast_cli(a->fd, "[Binded to: %s]\n", ast_str_buffer(zmq_data.connection_string));
 ast_cli(a->fd, "\t%d call%sexecuted so far.\n", calls, calls>1 ? "s " : " ");

 return CLI_SUCCESS;
}

static struct ast_cli_entry zmq_manager_status_cli[] = {
 AST_CLI_DEFINE(handle_cli_zmq_manager_status, "Shows useful stats about zmq manager usage"),
};

/*
 * Given a key, extracts its value and checks if it is a string.
 * Returns NULL if value is not a string.
 */
static const char *
get_string_or_null(json_t *obj, char *key) {
    json_t *tmp_obj = NULL;
    
    if (!json_is_object(obj))
        return NULL;

    tmp_obj = json_object_get(obj, key);
    
    if (json_is_null(tmp_obj) || !json_is_string(tmp_obj))
        return NULL;
    
    const char *tmp = json_string_value(tmp_obj);
    free(tmp_obj);
    return tmp;
}

/*
 * Extracts variables form the json dict. Variable is another dictionary
 * containing the var name (the key) and it's value (the value). This function
 * returns an ast_variable struct as the original one.
 */
struct ast_variable *
json_get_variables(json_t *data)
{
    void *iter = NULL;
    const char *key, *val_str;
    json_t *vars = NULL, *val = NULL;
    struct ast_variable *head = NULL, *cur;


    if (!json_is_object(data))
        goto end;
        
    vars = json_object_get(data, "Variable");
    
    if (!json_is_object(vars))
        goto end;

    iter = json_object_iter(vars);
    
    if (!iter)
        goto end;
    
    while(iter) {
        key = json_object_iter_key(iter);
        val = json_object_iter_value(iter);
        iter = json_object_iter_next(vars, iter);
        
        val_str = json_is_string(val) ? json_string_value(val) : NULL;
            
        if (!key || !val_str || ast_strlen_zero(key) || ast_strlen_zero(val_str)) {
            continue;
        }
        
        DEBUG("key, val: (%s, %s)\n", key, val_str);
        cur = ast_variable_new(key, val_str, "");
        cur->next = head;
        head = cur;
    }

end:
    free(iter);
    free(vars);
    free(val);
    return head;
}

/*
 * Does the originate using almost the same function implemented in 
 * main/manager.c. The main differences are that we parse a json object
 * and we don't have any application support so far. 
 *
 * What's left?
 * 
 * - Permissions (implemented globally)
 * - Applications
 */
static int
do_originate(json_t *m) {
    
    // DEBUG("%s\n", "Getting Channel");
    const char *name = get_string_or_null(m, "Channel");;
    
    // DEBUG("%s\n", "Getting Exten");
    const char *exten = get_string_or_null(m, "Exten");
    
    // DEBUG("%s\n", "Getting Context");
    const char *context = get_string_or_null(m, "Context");
    
    // DEBUG("%s\n", "Getting Priority");
    const char *priority = get_string_or_null(m, "Priority");
    
    // DEBUG("%s\n", "Getting Timeout");
    const char *timeout = get_string_or_null(m, "Timeout");
    
    // DEBUG("%s\n", "Getting CallerID");
    const char *callerid = get_string_or_null(m, "CallerID");
    
    // DEBUG("%s\n", "Getting Account");
    const char *account = get_string_or_null(m, "Account");
    
    // DEBUG("%s\n", "Getting Application");
    const char *app = get_string_or_null(m, "Application");
    
    // DEBUG("%s\n", "Getting Data");
    const char *appdata = get_string_or_null(m, "Data");
    
    // DEBUG("%s\n", "Getting Async");
    const char *async = get_string_or_null(m, "Async");
    
    // DEBUG("%s\n", "Getting ActionID");
    const char *id = get_string_or_null(m, "ActionID");
    
    // DEBUG("%s\n", "Getting Codecs");
    const char *codecs = get_string_or_null(m, "Codecs");
    
    struct ast_variable *vars;
    char *tech, *data;
    char *l = NULL, *n = NULL;
    int pi = 0;
    int res = 0;
    int to = 30000;
    int reason = 0;
    char tmp[256];
    char tmp2[256];
    
#if ASTERISK_VERSION_NUM > 10803
    struct ast_format_cap *cap = ast_format_cap_alloc_nolock();
    struct ast_format tmp_fmt;
    
    DEBUG("%s\n", "Checking attributes before calling");
    
    if (!cap) {
        res = -1;
        goto cleanup;
    }
    ast_format_cap_add(cap, ast_format_set(&tmp_fmt, AST_FORMAT_SLINEAR, 0));
#else
    format_t cap = AST_FORMAT_SLINEAR;
#endif

    if (ast_strlen_zero(name)) {
        res = 0;
        goto cleanup;
    }
    if (!ast_strlen_zero(priority) && (sscanf(priority, "%30d", &pi) != 1)) {
        if ((pi = ast_findlabel_extension(NULL, context, exten, priority, NULL)) < 1) {
            res = 0;
            goto cleanup;
        }
    }
    if (!ast_strlen_zero(timeout) && (sscanf(timeout, "%30d", &to) != 1)) {
         res = 0;
         goto cleanup;
    }
    
    ast_copy_string(tmp, name, sizeof(tmp));
    tech = tmp;
    data = strchr(tmp, '/');
    if (!data) {
        res = 0;
        goto cleanup;
    }
    *data++ = '\0';
    ast_copy_string(tmp2, callerid, sizeof(tmp2));
    ast_callerid_parse(tmp2, &n, &l);
    if (n) {
        if (ast_strlen_zero(n)) {
            n = NULL;
        }
    }
    if (l) {
        ast_shrink_phone_number(l);
        if (ast_strlen_zero(l)) {
            l = NULL;
        }
    }
    
    if (!ast_strlen_zero(codecs)) {
#if ASTERISK_VERSION_NUM > 10803
        ast_format_cap_remove_all(cap);
        ast_parse_allow_disallow(NULL, cap, codecs, 1);
#else
        cap = 0;
        ast_parse_allow_disallow(NULL, &cap, codecs, 1);
#endif
    }
    
    /* Allocate requested channel variables */
    vars = json_get_variables(m);

    DEBUG("%s\n", "About to call");
    if (exten && context && pi) {
        res = ast_pbx_outgoing_exten(tech, cap, data, to, context, exten, pi, &reason, 1, l, n, vars, account, NULL);
    } else {
        if (vars) {
            ast_variables_destroy(vars);
        }
        res = 0;
        goto cleanup;
    }
    
    if (!res) {
        calls +=1;
    } 

cleanup:
#if ASTERISK_VERSION_NUM > 10803
    ast_format_cap_destroy(cap);
#endif
    return res;
}

/**
 * Receives a new message from socket
 * and makes sure it is NULL terminated.
 */
static char *
ast_str_recv (void *socket) {
    zmq_msg_t message;
    zmq_msg_init (&message);
    zmq_recv (socket, &message, 0);
    int size = zmq_msg_size (&message);
    char *string = malloc (size + 1);
    memcpy (string, zmq_msg_data (&message), size);
    string [size] = 0;
    zmq_msg_close(&message);
    return string;
}


/**
 * Receives a new message (json serialized) from socket
 * and makes sure it is a json object.
 */
static json_t *
josn_recv(void *socket) {
    json_t *paramsObj;
    json_error_t error;
    
    char *msg = ast_str_recv(socket);
    paramsObj = json_loads(msg, 0, &error);
    
    if(!paramsObj)
        goto error;
    
    ast_free(msg);
    return paramsObj;

error:
    ast_free(msg);
    DEBUG("%s %s\n", "Error parsing json data", ast_str_buffer(msg));
    return NULL;
}

/**
 * Workers thread function.
 *
 * Process the message and calls the function based on the Command value.
 */
static void
ast_worker(void *data) {
    ast_mutex_lock( &workers_mutex );
    cur_workers++;
    ast_mutex_unlock( &workers_mutex );
    
    json_t *msg = (json_t *)data;
    
    if (!json_is_object(msg)) {
        DEBUG("%s %s\n", json_dumps(msg, JSON_INDENT(4)), "is not an object. Ignoring");
        goto leave;
    }

    DEBUG("%s\n", json_dumps(msg, JSON_INDENT(4)))
    json_t *command = json_object_get(msg, "Command");
    
    if (!json_is_string(command)) {
        DEBUG("%s\n", "Json is not an object. Ignoring");
        goto leave;
    }
    
    if (strcmp(json_string_value(command), "Originate") == 0) {
        int res = do_originate(msg);
    }
        
leave:
    free(msg);
    ast_mutex_lock( &workers_mutex );
    cur_workers--;
    DEBUG("Leaving Worker. %d workers running\n", cur_workers);
    ast_mutex_unlock( &workers_mutex );
}

/**
 * Workers thread's starter.
 *
 * Starts a new thread to process the message. Before starting the 
 * thread, it checks if there are any workers available.
 */
static void
ast_start_worker(json_t *msg) {
    pthread_t thread;
    
    if (zmq_data.max_workers!=0) {
        for(;;) {
            sleep(0.5);
            if (cur_workers<zmq_data.max_workers)
                break;
        }
    }
    if (ast_pthread_create_background( &thread, NULL, ast_worker, msg)) {
         ERROR("Unable to launch worker thread: %s\n", strerror(errno));
     }
}

/**
 * Main thread function.
 *
 * Binds to the connection_string and waits for new messages.
 */
static void 
ast_zmq_thread(void *data) {
    void *context = zmq_init (1);
    struct ast_zmq_pthread_data *pvt = (struct ast_zmq_pthread_data *)data;
    
    
    DEBUG("%s\n", "0MQ Thread");
    DEBUG("Connection string: %s\n", ast_str_buffer(pvt->connection_string));


    void *responder = zmq_socket (context, ZMQ_REP);
    if (responder==NULL) {
        ERROR("Couldn't created the new socket [%s]\n", strerror(errno));
        goto leave;
    }
    
    if (zmq_bind (responder, ast_str_buffer(pvt->connection_string))==-1) {
        ERROR("Couldn't bind [%s]\n", strerror(errno));
        goto leave;
    }
    binded = 1;
    

    DEBUG("%s\n", "About to start the loop");
    while (1) {
        //  Wait for next request from client
        DEBUG("%s\n", "Ready for a new message");
        ast_start_worker(josn_recv(responder));
        
        zmq_msg_t reply;
        zmq_msg_init_data (&reply, "Worker Started", 5, NULL, NULL);
        zmq_send (responder, &reply, 0);
        zmq_msg_close (&reply);
        
    }
    
leave:
    //  We never get here but if we did, this would be how we end
    zmq_close (responder);
    zmq_term (context);
}

/**
 * Stops the main thread if there's one.
 */
static bool
ast_zmq_stop() {
    
     /* Shutdown a running server if there is one */
    if (zmq_data.master != AST_PTHREADT_NULL) {
         pthread_cancel(zmq_data.master);
         pthread_kill(zmq_data.master, SIGURG);
         pthread_join(zmq_data.master, NULL);
         return true;
    }
    return false;
}

/**
 * Main thread starter.
 *
 * Starts the main thread.
 */
static int 
ast_zmq_start(struct ast_zmq_pthread_data *data) 
{
    
    if (ast_zmq_stop())
        DEBUG("%s\n", "There was a server running. Stopping it before starting a new one");
        
    DEBUG("%s\n", "About to start the thread");

     if (ast_pthread_create_background(&data->master, NULL, data->fn, data)) {
         ERROR("Unable to launch thread for %s on %s: %s\n",
             data->name,
             ast_str_buffer(data->connection_string),
             strerror(errno));
         goto error;
     }
     return 1;

error:
 close(data->accept_fd);
 return -1;

}


static int 
_unload_module(int reload)
{
    ast_zmq_stop();
    ast_mutex_destroy(&workers_mutex);
    return 0;
}

static int 
_load_module(int reload)
{
    DEBUG("%s\n", "Starting zmq manager module load.");
    
    int res;
    struct ast_config *cfg;
    struct ast_variable *var;
    struct ast_flags config_flags = { reload ? CONFIG_FLAG_FILEUNCHANGED : 0 };
    
    DEBUG("%s\n", "Loading zmq manager Config");
    if (!(cfg = ast_config_load(config, config_flags)) || cfg == CONFIG_STATUS_FILEINVALID) {
        ast_log(LOG_WARNING, "Unable to load config for zmq manager: %s\n", config);
        return AST_MODULE_LOAD_SUCCESS;
    } else if (cfg == CONFIG_STATUS_FILEUNCHANGED)
        return AST_MODULE_LOAD_SUCCESS;
    
    if (reload) {
        _unload_module(1);
    }
    
    DEBUG("%s\n", "Browsing zmq manager Global");
    var = ast_variable_browse(cfg, "global");
    if (!var) {
        return AST_MODULE_LOAD_SUCCESS;
    }
    
    res = 0;
    
    res |= load_config_string(cfg, "global", "connection_string", &zmq_data.connection_string, "tcp://*:967");
    res |= load_config_number(cfg, "global", "max_workers", &zmq_data.max_workers, 0);
    
    if (res < 0) {
        return AST_MODULE_LOAD_FAILURE;
    }
    
    DEBUG("%s %s\n", "Got connection string", ast_str_buffer(zmq_data.connection_string));
    DEBUG("%s %d\n", "Number of workers:", zmq_data.max_workers);
    
    // strncpy(opts.host, ast_str_buffer(address), 255);

    ast_config_destroy(cfg);
    if (res < 0) {
        ERROR("%s\n", "Module Load failed.");
        return AST_MODULE_LOAD_FAILURE;
    }
    // Let's start the workers_mutext
    ast_mutex_init(&workers_mutex);
    
    DEBUG("%s\n", "About to call ast_zmq_start");
    res = ast_zmq_start(&zmq_data);
    if (res) {
        res = ast_cli_register_multiple(zmq_manager_status_cli, sizeof(zmq_manager_status_cli) / sizeof(struct ast_cli_entry));
    }

    return res;
}

static int 
load_module(void)
{
    return _load_module(0);
}

static int 
unload_module(void)
{
    return _unload_module(0);
}

static int 
reload(void)
{
    return _load_module(1);
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_DEFAULT, "Manager based on zmq message",
    .load = load_module,
    .unload = unload_module,
    .reload = reload,
    );
