var mgmt = require('../lib/mgmt_console');

module.exports = {
  stat_matches: function(test) {
    test.expect(8);
    stat_vertical = {'a.b':1,'a.c':1,'c':1};
    
    //test function
    f = function (bucket) { return mgmt.existing_stats(stat_vertical, bucket) }

    //empties
    test.deepEqual(f('d'), []);
    test.deepEqual(f('a'), []);
    test.deepEqual(f('c.a'), []);
    test.deepEqual(f('c.*'), []);
    test.deepEqual(f(''), []);
    
    //single matches
    test.deepEqual(f('a.b'), ['a.b']);
    test.deepEqual(f('c'), ['c']);
    
    //multiple matches
    test.deepEqual(f('a.*'), ['a.b', 'a.c']);
    
    test.done();
  },
  
  stat_deletes: function(test) {
    test.expect(8);
    
    var stream = {
        buffer : '',
        clear : function() { this.buffer = '' },
        write : function(to_write) { this.buffer += to_write },
        };
    
    stats_fixture = 
    
    //delete missing
    stat_vertical = {'a.b':1,'a.c':1,'d':1};
    stream.clear();
    mgmt.delete_stats(stat_vertical, ['e'], stream);
    
    test.deepEqual(stat_vertical, stats_fixture);
    test.equal(stream.buffer, 'metric e not found\nEND\n\n');
    
    //delete fully qualified
    stat_vertical = {'a.b':1,'a.c':1,'d':1};
    stream.clear();
    mgmt.delete_stats(stat_vertical, ['a.b'], stream);
    
    test.deepEqual(stat_vertical, {'a.c':1,'d':1});
    test.equal(stream.buffer, 'deleted: a.b\nEND\n\n');
    
    //delete folder
    stat_vertical = {'a.b':1,'a.c':1,'d':1};
    stream.clear();
    mgmt.delete_stats(stat_vertical, ['a.*'], stream);
    
    test.deepEqual(stat_vertical, {'d':1});
    test.equal(stream.buffer, 'deleted: a.b\ndeleted: a.c\nEND\n\n');

    //delete all
    stat_vertical = {'a.b':1,'a.c':1,'d':1};
    stream.clear();
    mgmt.delete_stats(stat_vertical, ['*'], stream);

    test.deepEqual(stat_vertical, {});
    test.equal(stream.buffer, 'deleted: a.b\ndeleted: a.c\ndeleted: d\nEND\n\n');
    
    test.done();
  },
}
