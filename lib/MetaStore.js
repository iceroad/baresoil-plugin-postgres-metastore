const _ = require('lodash'),
  assert = require('assert'),
  json = JSON.stringify,
  pgquery = require('./pgquery'),
  sql = require('./sql'),
  timer = require('./timer')
  ;


function makeErrorKeyById(tableName, tableId) {
  assert(_.isString(tableName) && tableName, 'table not specified.');
  if (tableId) {
    return json(['id', tableName, _.toString(tableId)]);
  }
}

function makeErrorKeyByKey(tableName, tableKey) {
  assert(_.isString(tableName) && tableName, 'table not specified.');
  if (tableKey) {
    return json(['key', tableName, _.toString(tableKey)]);
  }
}

class MetaStore {
  init(deps, cb) {
    this.dbParams_ = deps.Config.MetaStore.postgres;
    this.sqlLog_ = deps.EventLog.sql.bind(deps.EventLog);
    this.counters_ = {
      queries: 0,
      errors: 0,
    };
    return cb();
  }

  transaction_(ops, cb) {
    const bodyStatements = _.filter(_.map(ops, (op) => {
      const opType = op.operation;
      if (opType === 'get') {
        return sql.Get(op);
      }
      if (opType === 'insert') {
        return sql.Insert(op);
      }
      if (opType === 'update') {
        return sql.Update(op);
      }
      if (opType === 'delete') {
        return sql.Delete(op);
      }
      throw new Error(`Unsupported operation ${opType}`);
    }));

    if (!bodyStatements.length) {
      return cb(new Error('No body statements.'));
    }

    // Generate and execute transaction.
    const stmt = sql.Transaction(bodyStatements);
    this.sqlLog_('query', stmt);
    const queryTimer = timer();
    return pgquery(this.dbParams_, stmt, (err, results) => {
      const queryTimeMs = queryTimer.stop();
      if (err) {
        let codedError = {};
        try {
          codedError = JSON.parse(err.message);
        } catch (e) {
          console.warn(`Invalid Postgres error message: "${err.message}"`);
        }

        if (_.toInteger(err.code) === 23514) {
          // check_violation raised by no updated keys.
          return cb({
            code: 'conflict',
            message: `Conflict in table "${codedError.table}".`,
            table: codedError.table,
            key: codedError.key,
            id: codedError.id,
          });
        }

        if (_.toInteger(err.code) === 23505) {
          // unique_violation raised by conflicting key.
          return cb({
            code: 'conflict',
            message: `Conflict in table "${codedError.table}".`,
            table: codedError.table,
            key: codedError.key,
            id: codedError.id,
          });
        }

        console.error(err);
        return cb(err);
      }

      const rows = _.flatten(_.map(results, 'rows'));
      this.sqlLog_('result', rows, queryTimeMs);

      // Index result rows by table and key.
      const rowIdx = {};
      _.forEach(rows, (row) => {
        const keyById = makeErrorKeyById(row.table_name, row.table_id);
        const keyByKey = makeErrorKeyByKey(row.table_name, row.table_key);
        if (keyById) {
          rowIdx[keyById] = row;
        }
        if (keyByKey) {
          rowIdx[keyByKey] = row;
        }
      });

      // Match each query item to an output row, if present.
      const items = _.map(ops, (op) => {
        const keyById = makeErrorKeyById(op.table, op.id);
        const keyByKey = makeErrorKeyByKey(op.table, op.key);
        const row = rowIdx[keyById] || rowIdx[keyByKey];

        const kvPair = {
          table: op.table,
          key: op.key,
          id: op.id,
        };

        if (!row) {
          kvPair.exists = false;
        } else {
          kvPair.exists = row.value_exists ? true : false;
          if (row.table_id) {
            kvPair.id = _.toInteger(row.table_id);
          }
          if (row.table_key) {
            kvPair.key = _.toString(row.table_key);
          }
          if (row.value) {
            kvPair.value = JSON.parse(row.value.toString('utf-8'));
          }
          if (row.value_version) {
            kvPair.version = row.value_version.toString('base64');
          }
          if (row.value_modified) {
            kvPair.modified = _.toInteger(row.value_modified);
          }
        }
        return kvPair;
      });

      return cb(null, items);
    });
  }

  destroy(deps, cb) {
    return cb();
  }
}

MetaStore.prototype.$spec = {
  deps: ['Config', 'EventLog'],
  config: {
    type: 'object',
    desc: 'Options for a Postgres-based MetaStore implementation.',
    fields: {
      postgres: {
        type: 'object',
        desc: 'Postgres server options.',
        fields: {
          host: {
            type: 'string',
            desc: 'Database hostname.',
          },
          user: {
            type: 'string',
            desc: 'Database user.',
          },
          password: {
            type: 'string',
            desc: 'Database password.',
          },
          database: {
            type: 'string',
            desc: 'Database name.',
          },
          rootUser: {
            type: 'string',
            desc: 'Database root user.',
            optional: true,
          },
          rootPassword: {
            type: 'string',
            desc: 'Database root password.',
            optional: true,
          },
          rootDatabase: {
            type: 'string',
            desc: 'Root database name.',
            optional: true,
          },
        },
      },
    },
  },
  defaults: {
    postgres: {
      host: 'localhost',
      user: 'baresoil',
      database: 'baresoil',
      rootUser: 'postgres',
      rootDatabase: 'postgres',
    },
  },
};

module.exports = MetaStore;
