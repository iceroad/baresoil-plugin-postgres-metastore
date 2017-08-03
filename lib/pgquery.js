const _ = require('lodash'),
  assert = require('assert'),
  pg = require('pg')
  ;


const Pools = {};


function pgquery(hostParams, sql, cb) {
  assert(_.isObject(hostParams));
  assert(_.isString(sql));

  const poolId = _.filter([
    hostParams.host,
    hostParams.port,
    hostParams.user,
    hostParams.database,
  ]).join(':');

  //
  // Extract pool.
  //
  const pool = Pools[poolId] = Pools[poolId] || (new pg.Pool({
    host: hostParams.host,
    port: hostParams.port,
    database: hostParams.database,
    user: hostParams.user,
    password: hostParams.password,
    max: 3,
  }));

  //
  // Run query and return results.
  //
  pool.connect((err, client, done) => {
    if (err) {
      done(err); // return connection to pool.
      return cb(err);
    }

    client.query(sql, (err, res) => {
      done(err);
      return cb(err, res);
    });
  });
}

pgquery.escape = function pgescape(inStr) {
  assert(_.isString(inStr));
  return pg.Client.prototype.escapeLiteral(inStr);
};

module.exports = pgquery;
