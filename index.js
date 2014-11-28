/*************
my db functions
use:
var db = require('..');
var options = {
    dbtype: 'ibase',
    database: require('path').join( __dirname, './data.fdb' ) ,
    host: '127.0.0.1',     // default
    port: 3050,            // default
    user: 'SYSDBA',        // default
    password: 'masterkey'  // default
};

db.connect(options,function(err,connect){
    if(err) trow(err);
    
    var qoptions = {params:['need name',99]} //не обязателньый параметр
    connect.query('SELECT * FROM table WHERE name=? AND id=?',qoptions,function(err,rows){
        if(err) trow(err);
        console.log(rows);
    });
    
    var gen_name = 'new_id_test_table'; //название генератора
    var inc_val = 1;  //не обязателньый параметр прирощения генератора
    connect.next_id(gen_name,inc_val,function(err,gen_id){
        if(err) trow(err);
        console.log('next id:'+gen_id);
    });
    
    connect.json('json_table_name',function(err,jsonworker){
        jsonworker.set('key1',{name:'hehe',x:10,y:20},function(err){
            if(err) trow(err);
            console.log('save key1');
            jsonworker.get('key1',function(err,data){
                if(err) trow(err);
                console.log('load key1:'+JSON.stringify(data));
            });
        });
    });
});
*************/
'use strict';

module.exports.connect = function(options,fn){
    if (!fn) throw('fn(argument 2) must be a function');
    var o = options;
    if (!o || typeof(o) !=="object") return fn(new Error('undefined db options(argument 1)'));
    if (!o.dbtype || typeof(o.dbtype)!=="string") return fn(new Error('undefined db options.dbtype'));
    
    o.dbtype = o.dbtype.toLowerCase();
    switch (o.dbtype) {
        case 'ibase':
        case 'interbase':
        case 'firebird':
            o.dbtype = 'firebird';
            break;
        
        default:
           return fn(new Error('not found "'+o.dbtype+'"(options.dbtype) functions'));
    }
    
    var lib = require('./lib/'+o.dbtype+'.js');
    lib.connect(options,fn);
}
