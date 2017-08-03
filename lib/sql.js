const _ = require('lodash'),
  assert = require('assert'),
  digest = require('./digest'),
  json = JSON.stringify,
  stablejson = require('json-stable-stringify'),
  PGClient = require('pg').Client
  ;


function SqlLiteral(inStr) {
  return PGClient.prototype.escapeLiteral(inStr);
}


function ToPostgresValue(fv, fn) {
  //
  // Order below is important.
  //

  // Integers are passed on as bigint.
  if (_.isInteger(fv)) {
    return `${fv}::bigint`;
  }

  // Date objects are converted to epoch timestamps.
  if (fv instanceof Date) {
    return `${fv.getTime()}::bigint`;
  }

  // Strings are SQL-escaped.
  if (_.isString(fv)) {
    return SqlLiteral(fv);
  }

  // Buffers are Base64 encoded.
  if (Buffer.isBuffer(fv)) {
    const b64 = fv.toString('base64');
    return `DECODE(${SqlLiteral(b64)}, 'base64')`;
  }

  throw new Error(`Field "${fn}" is not a supported type.`);
}


function Transaction(bodyStatements) {
  assert(_.isArray(bodyStatements));
  _.forEach(bodyStatements, s => assert(_.isString(s)));
  const bodyStr = bodyStatements.join('\nUNION ALL\n');
  return `\
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
${bodyStr};
COMMIT;`;
}

function Get(op) {
  assert(op.id || op.key, 'require "id" or "key"');
  const codedError = json({
    table: op.table,
    key: op.key,
    id: op.id,
  });

  if (op.id) {
    return `\
(
  SELECT * FROM kvd_get_by_id(
    ${ToPostgresValue(op.table)},
    ${ToPostgresValue(op.id)},
    ${ToPostgresValue(codedError)}
  )
)`;
  }

  return `\
(
  SELECT * FROM kvd_get_by_key(
    ${ToPostgresValue(op.table)},
    ${ToPostgresValue(op.key)},
    ${ToPostgresValue(codedError)}
  )
)`;
}

function Insert(op) {
  op.id = op.id || _.random(1, Number.MAX_SAFE_INTEGER);
  op.encoding = 'json';
  const value = Buffer.from(stablejson(op.value || null), 'utf-8');
  const version = digest(value); // Buffer
  const codedError = json({
    table: op.table,
    key: op.key,
    id: op.id,
  });

  return `\
(
  SELECT * FROM kvd_insert(
    ${ToPostgresValue(op.table)},
    ${ToPostgresValue(op.id)},
    ${op.key ? ToPostgresValue(op.key) : 'NULL'},
    ${ToPostgresValue(op.encoding)},
    ${ToPostgresValue(value)},
    ${ToPostgresValue(version)},
    ${ToPostgresValue(codedError)}
  )
)`;
}


function Update(op) {
  op.encoding = 'json';
  const value = Buffer.from(stablejson(op.value || null), 'utf-8');
  const version = digest(value); // Buffer
  const codedError = json({
    table: op.table,
    key: op.key,
    id: op.id,

  });
  const ifVersion = Buffer.from(op.ifVersion, 'base64');

  if (op.id) {
    return `\
  (
    SELECT * FROM kvd_update_by_id(
      ${ToPostgresValue(op.table)},
      ${ToPostgresValue(op.id)},
      ${ToPostgresValue(ifVersion)},
      ${ToPostgresValue(op.encoding)},
      ${ToPostgresValue(value)},
      ${ToPostgresValue(version)},
      ${ToPostgresValue(codedError)}
    )
  )`;
  }

  return `\
(
  SELECT * FROM kvd_update_by_key(
    ${ToPostgresValue(op.table)},
    ${ToPostgresValue(op.key)},
    ${ToPostgresValue(ifVersion)},
    ${ToPostgresValue(op.encoding)},
    ${ToPostgresValue(value)},
    ${ToPostgresValue(version)},
    ${ToPostgresValue(codedError)}
  )
)`;
}


function Delete(op) {
  const codedError = json({
    table: op.table,
    key: op.key,
    id: op.id,
  });
  let ifVersion;
  if (op.ifVersion) {
    ifVersion = Buffer.from(op.ifVersion, 'base64');
  }

  if (op.id) {
    return `\
  (
    SELECT * FROM kvd_delete_by_id(
      ${ToPostgresValue(op.table)},
      ${ToPostgresValue(op.id)},
      ${ifVersion ? ToPostgresValue(ifVersion) : 'NULL'},
      ${ToPostgresValue(codedError)}
    )
  )`;
  }

  return `\
(
  SELECT * FROM kvd_delete_by_key(
    ${ToPostgresValue(op.table)},
    ${ToPostgresValue(op.key)},
    ${ifVersion ? ToPostgresValue(ifVersion) : 'NULL'},
    ${ToPostgresValue(codedError)}
  )
)`;
}

module.exports = {
  Delete,
  Get,
  Insert,
  Transaction,
  Update,
};
