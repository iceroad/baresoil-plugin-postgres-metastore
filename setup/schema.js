module.exports = {
  columns: {
    table_name: 'VARCHAR(64) NOT NULL',
    table_id: 'BIGINT NOT NULL',
    table_key: 'VARCHAR(256)',
    value: 'BYTEA NOT NULL',
    value_encoding: 'VARCHAR(128) DEFAULT \'json\'',
    value_version: 'BYTEA NOT NULL',
    value_modified: 'BIGINT DEFAULT (1000 * EXTRACT(EPOCH FROM now()))::bigint',
  },
  primaryKey: 'PRIMARY KEY (table_name, table_id)',
  indices: [
    {
      name: 'uniq_key',
      unique: true,
      columns: ['table_name', 'table_key'],
      where: 'table_key IS NOT NULL',
    },
  ],
};
