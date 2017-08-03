const _ = require('lodash'),
  chalk = require('chalk'),
  pgquery = require('../lib/pgquery')
  ;


function createRole(dbParams, username, password, cb) {
  console.log(`+ Ensuring database role ${chalk.green(username)} exists...`);

  const qStr = 'SELECT rolname FROM pg_roles';
  pgquery(dbParams, qStr, (err, result) => {
    if (err) return cb(err);
    const rows = result.rows;
    const roles = _.map(rows, 'rolname');

    // Check if the role already exists.
    if (_.includes(roles, username)) {
      console.log(`+ Database role ${username} exists.`);
      return cb();
    }

    // Fix error: create an unprivileged "baresoil" user.
    const qStr = [
      'BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;',
      'DO',
      '$body$',
      'BEGIN',
      '  IF NOT EXISTS (',
      '      SELECT *',
      '      FROM   pg_catalog.pg_user',
      `      WHERE  usename = '${username}'`,  // yes, "usename" not "username"
      '  ) THEN',
      `      CREATE ROLE baresoil LOGIN PASSWORD '${password}';`,
      '  END IF;',
      'END',
      '$body$;',
      'END',
    ].join('\n');

    pgquery(dbParams, qStr, (err) => {
      if (err) return cb(err);
      console.log(`+ Created database role "${chalk.bold.green(username)}".`);
      return cb();
    });
  });
}

module.exports = createRole;
