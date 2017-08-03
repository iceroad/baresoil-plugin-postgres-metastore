const _ = require('lodash'),
  async = require('async'),
  chalk = require('chalk'),
  pgquery = require('../lib/pgquery'),
  StoredProcedures = require('./plpgsql')
  ;


function createStoredProcedures(dbParams, tableName, cb) {
  const runners = _.map(StoredProcedures, sp => (cb) => {
    console.log(`++ Updating stored procedure "${chalk.cyan(sp.name)}"...`);
    return pgquery(dbParams, sp.sql.replace(/kvd_metastore/mg, tableName), cb);
  });
  return async.series(runners, cb);
}

module.exports = createStoredProcedures;
