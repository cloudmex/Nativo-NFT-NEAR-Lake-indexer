let AWS = require("aws-sdk");
let postgres = require("postgres");

let secretsManager = new AWS.SecretsManager();

let { SM_EXAMPLE_DATABASE_CREDENTIALS, URL_RDS_PROXY } = process.env;

async function handler(event) {
    let sm = await secretsManager.getSecretValue({ SecretId: SM_EXAMPLE_DATABASE_CREDENTIALS }).promise();
    let credentials = JSON.parse(sm.SecretString);

    let connectionConfig = {
        host: URL_RDS_PROXY,
        port: credentials.port,
        username: credentials.username,
        password: credentials.password,
        database: credentials.dbname,
        ssl: true,
    };

    let sql = postgres(connectionConfig);
    let payload = {};

    let collectionColumns = event.info.selectionSetList.filter((item) => !item.startsWith("token"));
    let [collection] = await sql`SELECT ${sql(collectionColumns)} FROM collection WHERE collectionid = ${event.arguments.id}`;

    payload = { ...collection };

    if (event.info.selectionSetList.some((item) => item === "token")) {
        let tokenColumns = event.info.selectionSetList.filter((item) => item.startsWith("token/")).map((item) => item.split("/")[1]);
        let [token] = await sql`SELECT ${sql(tokenColumns)} FROM token WHERE collectionid = ${collection.id}`;

        payload.token = { ...token };
    }

    // if (event.info.selectionSetList.some((item) => item === "car/parking")) {
    //     let carParkingColumns = event.info.selectionSetList.filter((item) => item.startsWith("car/parking/")).map((item) => item.split("/")[2]);

    //     if (carParkingColumns.every((col) => parking[col])) {
    //         payload.car.parking = {};
    //         carParkingColumns.forEach((col) => {
    //             payload.car.parking[col] = parking[col];
    //         });
    //     } else {
    //         let parkingExtraColumns = carParkingColumns.filter((item) => !parkingColumns.includes(item));
    //         let [parkingExtraFields] = await sql`SELECT ${sql(parkingExtraColumns)} FROM Parking WHERE id = ${event.arguments.id}`;
    //         payload.car.parking = { ...parking, ...parkingExtraFields };
    //     }
    // }

    await sql.end({ timeout: 0 });

    return payload;
}

module.exports = { handler };