var should = require('should');

var postgresMapper = require('../model/postgresMapper.js');

describe('postgresMapper',function(){
  describe('invertMap',function() {
    it('should invert a map',function(bddone){
      var map = {keys:{a:'b',b:'c'}};
      postgresMapper.invertMap(map);
      should.exist(map.invertKeys);
      should(map.invertKeys).eql({b:'a',c:'b'});
      bddone();
    })
    it('should invert a map and lowercase all',function(bddone){
      var map = {keys:{a:'B',b:'C'}};
      postgresMapper.invertMap(map);
      should.exist(map.invertKeys);
      should(map.invertKeys).eql({b:'a',c:'b'});
      bddone();
    })
    it('should fail if not bijective',function(bddone){
      var map = {keys:{a:'b',b:'b'}};
      (function() {postgresMapper.invertMap(map)}).should.throw(Error);
      bddone();
    })
    it('should fail if not bijective in lowerCase Situations',function(bddone){
      var map = {keys:{a:'B',b:'b'}};
      (function() {postgresMapper.invertMap(map)}).should.throw(Error);
      bddone();
    })
  })
})