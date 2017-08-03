const fs = require('fs'),
  path = require('path')
;

function readSql(basename) {
  return {
    name: basename,
    sql: fs.readFileSync(path.resolve(__dirname, `${basename}.sql`), 'utf-8'),
  };
}

module.exports = [
  readSql('drop_old_functions'),
  readSql('kvd_get'),
  readSql('kvd_insert'),
  readSql('kvd_update'),
  readSql('kvd_delete'),
];
