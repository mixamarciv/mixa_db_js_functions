'use strict';

var fb = require("node-firebird");

module.exports.connect = function(conn_options,conn_fn){
    return new fb_functions(conn_options,conn_fn);
}

function fb_functions(conn_options,conn_fn) {
    if (!conn_fn) throw('conn_fn(argument 2) must be a function');
    if (!conn_options || typeof(conn_options)!=="object") return conn_fn(new Error('conn_options(argument 1) must be object'));
    
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
    
}
