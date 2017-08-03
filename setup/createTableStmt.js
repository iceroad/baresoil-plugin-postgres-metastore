const _ = require('lodash'),
  schema = require('./schema')
;


function CreateTableStmt(tableName) {
  const colDefinitions = _.map(schema.columns, (colDef, colName) => {
    return `  ${colName} ${colDef}`;
  }).join(',\n');

  const idxDefinitions = _.map(schema.indices, (idxDef) => {
    return `\
CREATE ${idxDef.unique ? 'UNIQUE ' : ''}INDEX IF NOT EXISTS ${idxDef.name}
ON ${tableName}
(${idxDef.columns.join(', ')})
${idxDef.where ? `WHERE ${idxDef.where}` : ''}
;`;
  }).join('\n');

  return `\
CREATE TABLE IF NOT EXISTS ${tableName} (
${colDefinitions},
${schema.primaryKey}
);
${idxDefinitions}

`;
}

module.exports = CreateTableStmt;
