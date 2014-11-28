'use strict';

var fb = require("node-firebird");

module.exports.connect = function(conn_options,conn_fn){
    return new fb_functions(conn_options,conn_fn);
}

function fb_functions(conn_options,conn_fn) {
    if (!conn_options || typeof(conn_options)!=="object") return conn_fn(new Error('conn_options(argument 1) must be object'));
    if (!conn_fn || typeof(conn_fn)!=="function") throw('conn_fn(argument 2) must be a function');
    
    var db_conn = {};
    var db_functions = {};
    db_functions.db_conn = db_conn;

    fb.attach(conn_options,function(err,p_db_conn){
        if (err) {
            err.conn_options = conn_options;
            return conn_fn(err);
        }
        db_conn = p_db_conn;
        db_functions.conn_options = conn_options;
        conn_fn(null,db_functions);
    });
    
    db_functions.close = function close(fn){
        db_conn.detach(fn);
    }
    
    db_functions.query = function(query_str,options,fn){
        if (!query_str || typeof(query_str)!=="string") return fn(new Error('query(argument 1) must be string'));
        if (!fn){
            if(typeof(options)=="function") {
                fn = options;
                options = {};
            }else{
                fn = function(){};
            }
        }
        
        var args = [query_str];
        if (options.params && typeof(options.params)=='array') {
            args.push(options.params);
        }
        
        args.push(function(err,rows){
            if (err) {
                err.query = query_str;
                err.conn_options = db_functions.conn_options;
                return fn(err);
            }
            fn(null,rows);
        });
        
        db_conn.query.apply(db_conn,args);
    }

    db_functions.next_id = function (gen_name,inc_val,fn){
        if (!gen_name || typeof(gen_name)!=="string") return fn(new Error('gen_name(argument 1) must be string'));
        if (!fn){
            if(typeof(inc_val)=="function") {
                fn = inc_val;
                inc_val = 1;
            }else{
                fn = function(){};
            }
        }
        var query_str = "SELECT gen_id("+gen_name+","+inc_val+") AS new_id FROM rdb$database";
        db_conn.query(query_str,function(err,rows){
            if (err) {
                err.query = query_str;
                err.conn_options = db_functions.conn_options;
                return fn(err);
            }
            var id = rows[0].new_id;
            fn(null,id);
        });
    }
    
    db_functions.json = function (table_name,fn){
        if (!table_name || typeof(table_name)!=="string") return fn(new Error('table_name(argument 1) must be string'));
        if (!fn || typeof(fn)!=="function") return fn(new Error('fn(argument 2) must be function'));
        json_check_table(db_conn,table_name,function(err){
            if (err) return fn(err);
            return new json_create_worker(db_conn,table_name,fn);
        });
        
    }
    
    db_functions.json_check_table = function (table_name,fn){
        if (!table_name || typeof(table_name)!=="string") return fn(new Error('table_name(argument 1) must be string'));
        if (!fn || typeof(fn)!=="function") return fn(new Error('fn(argument 2) must be function'));
        return json_check_table(db_conn,table_name,fn);
    }
}

function json_create_worker(db_conn,table_name,fnEnd) {
    var json_functions = {};
    
    json_functions.set = function(key,data,fn){
        var jsdata_str = {};
        try {
            jsdata_str = JSON.stringify(data);
        } catch(err) {
            return fn(err);
        }
        var q = "SELECT COUNT(*) AS cnt FROM "+table_name+" WHERE key='"+key+"'";
        db_conn.query(q,[key],function(err,rows){
            if (err) return fn(err);
            console.log(rows);
            var cnt = rows[0].cnt;
            if (cnt>0) {
                var q_upd = "UPDATE "+table_name+" SET data=? WHERE key='"+key+"'";
                return db_conn.query(q_upd,[jsdata_str],fn);
            }
            var q_ins = "INSERT INTO "+table_name+"(key,data) VALUES(?,?)";
            return db_conn.query(q_ins,[key,jsdata_str],fn);
        });
    }
    
    json_functions.get = function(key,fn){
        var q = "SELECT data FROM "+table_name+" WHERE key=?";
        db_conn.query(q,[key],function(err,rows){
            if (err) return fn(err);
            var str_data = rows[0].data;
            var data = {};
            try {
                data = JSON.parse(str_data);
            } catch(err) {
                return fn(err);
            }
            return fn(null,data);
        });
    }
    
    fnEnd(null,json_functions);
}

function json_check_table(db_conn,table_name,fn) {
    table_name = table_name.toUpperCase();
    //проверяем сущестрование таблицы, и если она не существует то создаем её
    var query_str = "SELECT COUNT(*) AS cnt FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = UPPER(TRIM('"+table_name+"'))";
    db_conn.query(query_str,function(err,rows){
        if (err) return fn(err);
        var cnt = rows[0].cnt;
        if (cnt>0) return fn(null);  //все отлично, таблица уже существует
        
        var qs = [
            "CREATE TABLE "+table_name+"( \n"+
            "  key INTEGER, \n"+
            "  data VARCHAR(8000), \n"+
            "  date_create  TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \n"+
            "  date_update  TIMESTAMP DEFAULT CURRENT_TIMESTAMP \n"+
            ")",
            
            "CREATE UNIQUE INDEX "+table_name+"_IDX1 ON "+table_name+"(KEY)",
            
            "CREATE OR ALTER TRIGGER "+table_name+"_BU0 FOR "+table_name+" \n"+
            "ACTIVE BEFORE UPDATE POSITION 0 \n"+
            "AS \n"+
            "BEGIN \n"+
            "  new.date_update = current_timestamp; \n"+
            "END"
        ];
        
        run_next_query(db_conn,qs,0,fn);
    });
}

function run_next_query(db_conn,qs,i,fn) {
    var q = qs[i++];
    db_conn.query(q,function(err){
        if (err) return fn(err);
        if (i >= qs.length) return fn(null);
        return run_next_query(db_conn,qs,i,fn);
    });
}
