const chalk = require('chalk'),
  pgquery = require('../lib/pgquery')
;

function grantTableAccess(dbParams, dbName, username, cb) {
  console.log(`+ Allowing user ${username} access to database "${dbName}"...`);
  const qStr = `GRANT CREATE, CONNECT ON DATABASE ${dbName} TO ${username};`;
  pgquery(dbParams, qStr, cb);
}

module.exports = grantTableAccess;
