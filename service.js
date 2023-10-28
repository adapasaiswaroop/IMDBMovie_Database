import * as csv from 'fast-csv';
import knex from 'knex';

const db = knex({
  client: 'mysql',
  connection: {
    host: 'localhost',
    user: 'root',
    database: 'imdb',
    password: 'password'
  }
});
const readCsv = (path, options) => {
  return new Promise((resolve, reject) => {
    const data = [];
    csv
      .parseFile(path, options)
      .on('error', reject)
      .on('data', (row) => {
        data.push(row);
      })
      .on('end', () => {
        resolve(data);
      });
  });
};

export const csvToJson = async (fileName, tableHeaders) => {
  try {
    const data = await readCsv(`csv_files/${fileName}.csv`, {
      headers: tableHeaders,
      trim: true,
      skipRows: 1
    });
    return data;
  } catch (error) {
    throw error;
  }
};


export const insertRowsIntoTable = async (tableName, normalizedDataRows) => {
  try {
    await db.batchInsert(tableName, normalizedDataRows);
  } catch (error) {
    throw error;
  }
};

export const normalizeRows = (csvRows, key) => {
    const data = [];
    let isInserted = [];
    for (let index = 0; index < csvRows.length; index++) {
      if (!isInserted.includes(csvRows[index][key])) {
        data.push(csvRows[index]);
        isInserted.push(csvRows[index][key]);
      }
    }
    return data;
  };
  
