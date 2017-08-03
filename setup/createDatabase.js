const _ = require('lodash'),
  chalk = require('chalk'),
  pgquery = require('../lib/pgquery')
  ;


function createDatabase(dbParams, dbName, cb) {
  console.log(`+ Ensuring database ${chalk.green(dbName)} exists...`);

  let qStr = [
    'SELECT datname FROM pg_database',
    'WHERE datistemplate = false',
  ].join(' ');

  return pgquery(dbParams, qStr, (err, result) => {
    if (err) return cb(err);

    // Check if database already exists.
    const allDbNames = _.map(result.rows, 'datname');
    if (_.includes(allDbNames, dbName)) {
      console.log(`+ Database ${chalk.green(dbName)} exists.`);
      return cb();
    }

    // Create the database.
    qStr = `CREATE DATABASE ${dbName} WITH ENCODING 'UTF8';`;
    return pgquery(dbParams, qStr, (err) => {
      if (err) return cb(err);
      console.log(`+ Database ${chalk.green(dbName)} created.`);
      return cb();
    });
  });
}

module.exports = createDatabase;
