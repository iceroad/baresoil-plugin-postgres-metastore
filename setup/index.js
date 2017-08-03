const _ = require('lodash'),
  assert = require('assert'),
  async = require('async'),
  chalk = require('chalk'),
  createDatabase = require('./createDatabase'),
  createRole = require('./createRole'),
  createStoredProcedures = require('./createStoredProcedures'),
  createTable = require('./createTable'),
  grantDatabaseAccess = require('./grantDatabaseAccess')
  ;


function SetupDatabase(postgresParams, cb) {
  assert(_.isObject(postgresParams));
  assert(_.isFunction(cb));

  console.log(
    `Setting up postgres database ${chalk.green(postgresParams.database)} ` +
      `on server ${chalk.green(postgresParams.user)}@${chalk.green(postgresParams.host)}...`);

  // Generate connection parameters for system user.
  const pgParamsSys = {
    host: postgresParams.host,
    port: postgresParams.port,
    user: postgresParams.rootUser,
    password: postgresParams.rootPassword,
    database: postgresParams.rootDatabase,
  };

  // Generate connection parameters to database.
  const dbName = postgresParams.database || '';
  if (!dbName.match(/^[a-z0-9_]+$/i)) {
    return cb(new Error(`Invalid postgres database name: ${dbName}`));
  }
  const pgParamsUser = {
    host: postgresParams.host,
    port: postgresParams.port,
    user: postgresParams.user,
    password: postgresParams.password,
    database: postgresParams.database,
  };

  // Setup database.
  async.series([
    // As root user
    cb => createDatabase(pgParamsSys, dbName, cb),
    cb => createRole(pgParamsSys, postgresParams.user, postgresParams.password, cb),
    cb => grantDatabaseAccess(pgParamsSys, postgresParams.database, postgresParams.user, cb),

    // As regular user
    cb => createTable(pgParamsUser, 'metastore', cb),
    cb => createStoredProcedures(pgParamsUser, 'metastore', cb),
  ], cb);
}


function SetupPostgresCmd(base, args) {
  const config = base.getConfig(args);
  SetupDatabase(config.MetaStore.postgres, (err) => {
    if (err) {
      console.error(err);
      if (err.code === 'ER_BAD_DB_ERROR') {
        console.error(
          `Database ${config.MetaStore.postgres.database} not present on server.
Please run command "${chalk.bold(`CREATE DATABASE ${config.MetaStore.postgres.database}`)}" \
on postgres server ${chalk.bold(config.MetaStore.postgres.host)}`);
      } else {
        console.error(err);
      }
      return process.exit(1);
    }

    console.log(chalk.green('Postgres database setup completed.'));
    return process.exit(0);
  });
}

SetupPostgresCmd.$core = SetupDatabase;

module.exports = SetupPostgresCmd;
