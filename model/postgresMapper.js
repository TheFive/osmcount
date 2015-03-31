var pg     = require('pg');
var debug  = require('debug')('postgresMapper');
var should = require('should');


var config = require('../configuration.js');



exports.count = function(map,query,cb) {
  debug('count');
  var whereClause = ""
  for (var k in map.keys) {
  	if (typeof(query[k])!='undefined'){
  		var op = '=';
  		if (map.regex[k] == true) op = '~';
  		if (whereClause != '') whereClause += " and ";
  		whereClause += map.keys[k]+' '+op+" '"+query[k]+"'";
  	}
  }
  debug("generated where clause: %s",whereClause);
  if(whereClause != '') whereClause = "where "+whereClause;
  pg.connect(config.postgresConnectStr,function(err,client,pgdone) {
    if (err) {
      cb(err,null);
      pgdone();
      return;
    }
    client.query("select count(*) from "+map.tableName+ ' '+whereClause,
                                function(err,result) {
      should.not.exist(err);
      var count = result.rows[0].count;
      cb (null,count);
      pgdone();
      return;
    })
  })
}