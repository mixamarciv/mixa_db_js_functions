'use strict';

require('mocha');
var should = require('should');
var join = require('path').join;


var db = require('..');
var options = {
    dbtype: 'ibase',
    database: join( __dirname, './data1.fdb' ),
    host: '127.0.0.1',     // default
    port: 3050,            // default
    user: 'SYSDBA',        // default
    password: 'masterkey'  // default
};


var arr_queries = [
  'DELETE FROM test_table1',
  'DROP TABLE test_table1',
  'CREATE TABLE test_table1 (id INTEGER,data VARCHAR(8000))',
  'CREATE UNIQUE INDEX test_table1_idx1 ON test_table1 (id)',
  'DROP   SEQUENCE test_table1_id',
  'CREATE SEQUENCE test_table1_id',
  'ALTER  SEQUENCE test_table1_id RESTART WITH 100',
  'INSERT INTO test_table1(id,data) VALUES(gen_id(test_table1_id,1),\'test1\')',
  'INSERT INTO test_table1(id,data) VALUES(gen_id(test_table1_id,1),\'test2\')',
  'INSERT INTO test_table1(id,data) VALUES(gen_id(test_table1_id,1),\'test3\')'
];

describe('run firebird tests', function() {
    var conn = {};
    
    describe('connect()', function() {
        it('should return a conn functons', function(done) {
            db.connect(options,function(err,p_conn){
                should.not.exist(err);
                should.exist(conn);
                conn = p_conn;
                done();
            });
        });
    });
    
    for(var i=0;i<arr_queries.length;i++){
        describe('query'+(i+1)+'()', function(){
            var query = arr_queries[i];
            it('run: '+query, function(done){
                conn.query(query,function(err,rows){
                    should.not.exist(err);
                    done();
                });
            });
        });
    }
    
    describe('query()', function(){
        var query = 'SELECT id,data FROM test_table1';
        it('run: '+query, function(done){
            conn.query(query,function(err,rows){
                should.not.exist(err);
                
                should.exist(rows);
                rows.length.should.be.equal(3);
                should.exist(rows[0]);
                
                var row = rows[0];
                should.exist(row.id);
                should.exist(row.data);
                (row.data).should.be.equal('test1');
                
                row = rows[2];
                should.exist(row.id);
                should.exist(row.data);
                (row.data).should.be.equal('test3');
                
                done();
            });
        });
    });
    
    describe('next_id()', function() {
        it('generator should return 104', function(done) {
            conn.next_id('test_table1_id',function(err,id){
                should.not.exist(err);
                (id.high_ + id.low_).should.be.equal(104);
                done();
            });
        });
    });

    
    var json_q = {};
    describe('json()', function() {
        it('should return object for work with json records', function(done) {
            conn.query('DROP TABLE test_table6',function(err){
                conn.json('test_table6',function(err,json_tb){
                    should.not.exist(err);
                    json_q = json_tb;
                    conn.query('SELECT COUNT(*) AS cnt FROM test_table5',function(err,rows){
                        should.not.exist(err);
                        var cnt = rows[0].cnt;
                        done();
                    });
                });
            });
        });
    });
    
    var test_obj = {test1:1,test2:{aaa:1,bbb:2,ccc:3}};
    
    describe('json().set', function() {
        it('should set json data to db', function(done) {
            json_q.set('testkey1',test_obj, function(err){
                should.not.exist(err);
                done();
            });
        });
    });
    
    describe('json().get', function() {
        it('should get json data from db', function(done) {
            json_q.get('testkey1', function(err,data){
                should.not.exist(err);
                data.should.be.eql(test_obj);
                done();
            });
        });
    });

    
    describe('close()', function() {
        it('detach database', function(done) {
            conn.close(function(err){
                should.not.exist(err);
                done();
            });
        });
    });
    
});


