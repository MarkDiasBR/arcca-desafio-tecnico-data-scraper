import axios from 'axios';
import pg from 'pg';

export const hello = async (event) => {
  let runTime = Date.now();

  const allStoresReviewsArray = [];

  for (let i = 1; i <= 19; i++) {
    let timer = Date.now();
    console.log(`Fetching reviews for store ${i}...`)
    allStoresReviewsArray.push(await fetchReviews(i));

    console.log(`Done fetching reviews for store ${i}... (${((Date.now() - timer) / 1000).toFixed(2)} s)`)
  }

  for (let i = 1; i <= 19; i++) {
    let timer = Date.now();
    console.log(`Adding reviews in DB for store ${i}...`)
    await runDatabaseByReviewId(i, allStoresReviewsArray[i - 1]);
    console.log(`Done adding reviews in DB for store ${i}... (${((Date.now() - timer) / 1000).toFixed(2)} s)`)
  }

  console.log(`Tempo total de execução: (${((Date.now() - runTime) / 1000).toFixed(2)} s)`);
  console.log("event.name: ", event.name);

  const lambdaResponse = {
    statusCode: 200,
    body: JSON.stringify({
      tempoDeExecucao: `${((Date.now() - runTime) / 1000).toFixed(2)} s`,
      eventName: event.name
    })
  }

  return lambdaResponse;

  async function fetchReviews(storeNumber) {
    const query = findQueryByStoreNumber(storeNumber);
    let paginationToken = null;
    let responsesList = [];
    let response;
    let structuredResponse = [];
    const reviews = [];

    let maxPages = 3;
    do {
      const paginatedQuery = insertStringIntoString(query, paginationToken);
      response = await axios.get(paginatedQuery);

      structuredResponse = parseResponse(response.data);
      if (structuredResponse.length < 1) break;

      responsesList.push(structuredResponse);
      paginationToken = structuredResponse[1];
      maxPages--;
    } while (maxPages > 0 && paginationToken !== null);

    for (let responseUnit of responsesList) {
      const reviewArray = responseUnit[2];

      let page = 1

      for (let reviewUnit of reviewArray) {
        const review = [
          // nota
          reviewUnit[0][2][0][0],
          // googleID
          reviewUnit[0][1][4][0][13],
          //userName
          reviewUnit[0][1][4][0][4],
          // review (lógica: se [0][2] existe e [0][2] não é null, então o review existe, senão, review = null)
          reviewUnit[0][2][15] && reviewUnit[0][2][15][0][0].replaceAll(/\n$/g, '').replaceAll(/\n/g, ' ') || null,
          // ownerResponse
          reviewUnit[0][3][14] && reviewUnit[0][3][14][0][0].replaceAll(/\n$/g, '').replaceAll(/\n/g, ' ') || null
        ]

        reviews.push(review);
      }
    }

    console.log("store_id:", storeNumber, "reviews:", reviews.length)
    return reviews; // retorna um array de reviews de length igual a quantidade de reviews da loja. todos os reviews são da mesma loja (mesmo store_id)
  }

  async function runDatabaseByReviewId(id, reviews) {
    const { Pool, Client } = pg;
    const connectionString = 'postgresql://postgres:postpass@postgres-nema-reviews.c9wqu22m2tmd.us-east-1.rds.amazonaws.com:5432/nema';
    const pool = new Pool({
      connectionString,
      ssl: {
        rejectUnauthorized: false
      }
    });

    let client;

    try {
      client = await pool.connect();
      console.log("Connected to the database");
    } catch (err) {
      console.error('Error connecting to the database', err.stack);
      return; // Return early if connection failed
    }

    try {
      await client.query('BEGIN');
      console.log("BEGIN");

      // console.log("reviews: ", reviewsVariable)

      // Primeiro, insira ou atualize todos os usuários neste array de reviews (de uma dada loja)
      for (let review of reviews) {
        await client.query(`
        INSERT INTO "user" ("google_id", "name", "created_at", "updated_at")
        VALUES ($1, $2, NOW(), NULL)
        ON CONFLICT ("google_id") DO UPDATE
        SET "name" = EXCLUDED.name, "updated_at" = NOW()
        WHERE "user"."name" <> EXCLUDED.name;
        `, [review[1], review[2]]);
      }

      // Inserir todas as reviews desta loja na tabela
      for (let review of reviews) {

        const thisUserId = await fetchUserIdByGoogleId(review[1], client);

        // Insere as reviews na tabela
        await client.query(`
          INSERT INTO "review" ("user_id", "store_id", "rating", "review", "owner_reply")
          SELECT $1, $2, $3, $4, $5
          ON CONFLICT ON CONSTRAINT "unique_user_store" DO UPDATE
          SET "rating" = EXCLUDED.rating, "review" = EXCLUDED.review, "owner_reply" = EXCLUDED.owner_reply, "updated_at" = NOW()
          WHERE "review"."rating" <> EXCLUDED.rating OR "review"."review" <> EXCLUDED.review OR "review"."owner_reply" <> EXCLUDED.owner_reply;
        `, [thisUserId, id, review[0], review[3], review[4]]);
      }

      //TODO: Elimina todos os usuários que: user possui review com store_id dado, porém não estão no array atualizado de reviews desta mesma store_id e nem possuem alguma outra review, portanto, é deletado
      // const googleIdsArray = reviews.map(r => r[1]);
      // const placeholdersString = [...Array(googleIdsArray.length).keys()].map(i => `$${i + 1}`).join(', ');
      // await client.query(`
      //   DELETE FROM "user"
      //   WHERE "user"."google_id" NOT IN (${placeholdersString})
      //   AND NOT EXISTS (
      //     SELECT 1 FROM "review"
      //     WHERE "review"."user_id" = $${googleIdsArray.length + 1}
      //   )
      //   AND "review"."store_id" = $${googleIdsArray.length + 2}
      //   `, [...googleIdsArray, id, id]);

      await client.query('COMMIT');
      console.log("COMMIT")
    } catch (error) {
      await client.query('ROLLBACK');
      console.log("ROLLBACK")
      console.log(error);
    } finally {
      client.release();
      console.log('Database client released')
    }
  }

  async function fetchUserIdByGoogleId(googleId, client) {
    const response = await client.query(`
      SELECT "id" FROM "user"
      WHERE "google_id" = $1
    `, [googleId]);

    if (response.rows) {
      return response.rows[0].id;
    } else {
      // return false;
    }
  }

  function insertStringIntoString(originalString, insertedString) {

    const index = originalString.indexOf('!2s') + 3; // Find the index of "!2s" and add 4 to point after the 's'

    if (insertedString !== null && insertedString !== undefined) {
      return originalString.slice(0, index) + insertedString + originalString.slice(index);
    } else {
      return originalString;
    }
  }

  function parseResponse(jsonString) {
    // Remove the prefix ")]}'"
    const cleanedJson = jsonString.replace(/^\)\]\}\'/, '');

    // Parse the JSON string
    const parsedJson = JSON.parse(cleanedJson);

    // Return the parsed JSON
    return parsedJson;
  }

  function findQueryByStoreNumber(storeNumber) {
    switch (storeNumber) {
      case 0:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x74fd59ec2fcc94d%3A0x3e3a1e157cd365d4!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s7MnvZeKSBtGU5OUP7sK5sA0!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 1:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd5b81cec9b05%3A0xc9b595f28b3ca216!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1snLfvZYyyCoHS1sQPxZSTiAw!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 2:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd58a0cdc1487%3A0x4c1eb56d62eb469b!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sULvvZaypCf3S1sQP3cuBwAU!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 3:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd51fff4cc717%3A0x930f8a469526651c!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sl7vvZZqmIZvR1sQP8Y-omAY!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 4:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd50c9e3bdca3%3A0x2c4fc7ac213d2944!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s0rzvZZTpNoyi5OUPze2S4AI!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 5:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x998126c31120f5%3A0x97d5a5b3c285ecf6!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sNb3vZavQBKrT1sQPme2p6AI!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 6:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd54894ebaad1%3A0x122ca35f5ad3af7b!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sab3vZffzA8La1sQPwJGHyAk!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 7:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd5ec3fd19053%3A0x36965d5a25cc2!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1scb7vZZj_Co2H5OUPvbeguAg!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 8:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x997fd3ce25318b%3A0x17650611ede4f2c9!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sIsPvZb7_Oo_N1sQP2sm06Ak!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 9:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x997f8623975009%3A0x78aa8c59142594bc!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s8dvvZbCiMe735OUPn46V0AY!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 10:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x997f002be19a01%3A0x1b66a2e3c7e40887!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s_tzvZafXCI7Y1sQPiv23iAw!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 11:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x997f40dcd167a5%3A0x42282f47a424e9a6!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sbt3vZdjGFcrW1sQPpdab0Ao!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 12:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bd1e8dd384b8f%3A0x44ffeaa2ec164094!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1skN3vZdz9Edao1sQP_8SFmAE!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 13:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x998373996a193f%3A0xb7ce627a8de35c77!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1ssN3vZf-jL8XV1sQPkvCCyAs!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 14:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x9bdb1c68a1641d%3A0x3bb26d7260491551!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1sQd7vZae0O4LQ1sQP6LSpmAg!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 15:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0xb8176afd21d35d%3A0x3a75e6d1632d5757!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1shN7vZaGNN9CG4dUPqMCu6Ac!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 16:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x94ce57102cca443d%3A0x10c8b490a6e9948e!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s3OHvZayVFe-65OUP1dCvgAg!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 17:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x94ce43a22f527af3%3A0x5bd1cd75925efd2!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s2OHvZZ3NEP6J5OUPwMekyAY!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 18:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x952739b9ef7d6abb%3A0x2f6338bda4c94d6c!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s1eHvZf6oJoyi5OUPze2S4AI!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
      case 19:
        return 'https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&gl=br&pb=!1m7!1s0x952739757c644431%3A0x66cfb55158630b43!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s!5m2!1s1eHvZe-6JLbZ5OUPzqiW0A4!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen!4sbr!6m1!1i2!13m1!1e2';
    }
  }
};
