/*jshint node:true, laxcomma:true */

/**
 * delete_stats - delete all matching statistics
 *
 * Side effect notes: this function works by altering stats_type in place,
 *   and calls stream.write(str) to display user feedback.
 *
 * @param stats_type array of all statistics of this type (eg~ timers) to delete from
 * @param cmdline array of all requested deletions, which can be fully qualified,
 *   or end in a .* to delete a folder, like stats.temp.*
 * @param stream buffer output for for all outgoing user feedback
 */
exports.delete_stats = function(stats_type, cmdline, stream) {

  //for each metric requested on the command line
  for (const index in cmdline) {

    //get a list of deletable metrics that match the request
    deletable = existing_stats(stats_type, cmdline[index]);

    //warn if no matches
    if (deletable.length === 0) {
      stream.write("metric " + cmdline[index] + " not found\n");
    }

    //delete all requested metrics
    for (const del_idx in deletable) {
      delete stats_type[deletable[del_idx]];
      stream.write("deleted: " + deletable[del_idx] + "\n");
    }
  }
  stream.write("END\n\n");
};

/**
 * existing_stats - find fully qualified matches for the requested stats bucket
 *
 * @param stats_type array of all statistics of this type (eg~ timers) to match
 * @param bucket string to search on, which can be fully qualified,
 *   or end in a .* to search for a folder, like stats.temp.*
 *
 * @return array of fully qualified stats that match the specified bucket. if
 *   no matches, an empty array is a valid response
 */
function existing_stats(stats_type, bucket){
  matches = [];

  //special case: match a whole of stats
  if (bucket == "*") {
    return Object.keys(stats_type).slice()
  }

  //typical case: one-off, fully qualified
  if (bucket in stats_type) {
    matches.push(bucket);
  }

  //special case: match a whole 'folder' (and subfolders) of stats
  if (bucket.slice(-2) == ".*") {
    const folder = bucket.slice(0,-1);

    for (const name in stats_type) {
      //check if stat is in bucket, ie~ name starts with folder
      if (name.substring(0, folder.length) == folder) {
        matches.push(name);
      }
    }
  }

  return matches;
}

exports.existing_stats = existing_stats;
