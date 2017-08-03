const _ = require('lodash'),
  async = require('async'),
  chalk = require('chalk'),
  json = JSON.stringify,
  schema = require('./schema'),
  pgquery = require('../lib/pgquery'),
  createTableStmt = require('./createTableStmt')
  ;

function createTable(dbParams, tableName, cb) {
  async.auto({
    //
    // Get current tables.
    //
    tableExists: (cb) => {
      console.log(`+ Ensuring table ${chalk.green(tableName)} exists...`);
      const qStr = [
        'SELECT table_name FROM information_schema.tables ',
        'WHERE table_schema=\'public\'',
        `AND table_name = ${pgquery.escape(tableName)}`,
      ].join(' ');
      pgquery(dbParams, qStr, (err, result) => {
        if (err) return cb(err);
        return cb(null, result.rows.length);
      });
    },

    //
    // If table exists, get current columns.
    //
    tableSchemaCheck: ['tableExists', (deps, cb) => {
      if (!deps.tableExists) return cb();
      const qStr = [
        'SELECT column_name, data_type',
        'FROM INFORMATION_SCHEMA.COLUMNS',
        `WHERE table_name = ${pgquery.escape(tableName)}`,
      ].join('\n');
      pgquery(dbParams, qStr, (err, result) => {
        if (err) return cb(err);
        const curColNames = _.map(result.rows, 'column_name');
        const wantedColNames = _.keys(schema.columns);
        const missingCols = _.difference(wantedColNames, curColNames);
        if (missingCols.length) {
          return cb(new Error(
              `Table ${tableName} is missing columns: ${missingCols.join(',')}`));
        }
        const extraCols = _.difference(curColNames, wantedColNames);
        if (extraCols.length) {
          console.warn(
              `Table {$tableName} has unwanted columns: ${extraCols.join(', ')}`);
        }
        console.log(
          `+ Table ${chalk.green(tableName)} exists with all required columns.`);
        return cb();
      });
    }],

    //
    // Create table if it does not exist.
    //
    create: ['tableExists', (deps, cb) => {
      if (deps.tableExists) return cb();
      const qStr = createTableStmt(tableName);
      return pgquery(dbParams, qStr, (err) => {
        if (err) return cb(err);
        console.log(`+ Table ${chalk.green(tableName)} exists.`);
        return cb();
      });
    }],
  }, cb);
}

module.exports = createTable;
