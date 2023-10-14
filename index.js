import express from 'express';
import bodyParser from 'body-parser';
import { csvToJson, insertRowsIntoTable, normalizeRows } from './service.js';
import knex from 'knex';

const db = knex({
  client: 'mysql',
  connection: {
    host: 'localhost',
    port: 3306,
    user: 'root',
    database: 'imdb',
    password: 'password'
  }
});

const app = express();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const port = 3000;

app.get('/', async (req, res) => {
  let fileCounter = 1;
  for (const [key, object] of Object.entries(files)) {
    console.log(fileCounter);
    for (let j = 0; j < object.length; j++) {
      const dataFromCsv = await csvToJson(
        `file${fileCounter}`,
        object[j].columns
      );
      let normalizedData;
      if (object[j].ignoreNormalization) {
        normalizedData=dataFromCsv;
      }else{
        normalizedData = normalizeRows(dataFromCsv, object[j].key);
      }
      console.log("------",normalizedData);
      await insertRowsIntoTable(object[j].tableName, normalizedData);
    }
    fileCounter++;
  }
  res.status(200).send('Data inserted');
});


app.listen(port, () => {
  console.log(`app listening at http://localhost:${port}`);
});


const files = {
  file1Tables: [
    {
      tableName: 'movie',
      columns: [
        'movie_name',
        'budget',
        'release_date',
        'runtime',
        'grade_certificate',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined
      ],
      key: 'movie_name'
    },
    {
      tableName: 'rating',
      columns: [
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'rating_company',
        'r_description',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined
      ],
      key: 'rating_company'
    },
    {
      tableName: 'production_company',
      columns: [
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'production_name',
        'headquarters_location',
        'products',
        'key_people',
        undefined,
        undefined,
        undefined,
        undefined
      ],
      key: 'production_name'
    },
    {
      tableName: 'movie_productioncompany',
      columns: [
        'movie_name',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'production_name',
        undefined,
        undefined,
        undefined,
      ],
      key: 'movie_name',
      ignoreNormalization: true
    },
    {
      tableName: 'movie_rating',
      columns: [
        'movie_name',
        undefined,
        undefined,
        undefined,
        undefined,
        'rating_company',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
      ],
      key: 'movie_name',
      ignoreNormalization: true
    },
  ],
  file2Tables: [
    {
      tableName: 'genre',
      columns: [
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'genre_type',
        'description',
        undefined,
        undefined
      ],
      key: 'genre_type'
    },
    {
      tableName: 'movie_genre',
      columns: [
        'movie_name',
        undefined,
        undefined,
        undefined,
        undefined,
        'genre_type',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined
      ],
      key: 'movie_name',
      ignoreNormalization: true
    },
    {
      tableName: 'language',
      columns: [
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'language_nametype',
        'region'
      ],
      key: 'language_nametype'
    },
    {
      tableName: 'movie_language',
      columns: [
        'movie_name',
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'language_nametype',
        undefined
      ],
      key: 'movie_name',
      ignoreNormalization: true
    },
  ],
  file3Tables: [
    {
      tableName: 'cast_and_crew',
      columns: [
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        'person_fullname',
        'role'
      ],
      key: 'person_fullname'
    },
    {
      tableName: 'movie_cast_and_crew',
      columns: [
        'movie_name',
        undefined,
        undefined,
        undefined,
        undefined,
        'person_fullname',
        undefined
      ],
      key: 'movie_name',
      ignoreNormalization: true
    },

  ]
};

