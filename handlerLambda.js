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

    // let sql = postgres(connectionConfig);
    //Falta modifcar la conexion a la base de datos de postgreSQL
    //Tambien se tiene que realizar la adaptacion de las querys y recuperacion de informacion
    //Una manera de implementarlo esta en el siguiente enlace: https://ed.team/blog/como-usar-bases-de-datos-postgres-con-nodejs
    let payload = {};

    switch(event.info.fieldName){
        case "getCollectionGallery":
            let collectionColumns = event.info.selectionSetList.filter((item) => !item.startsWith("token"));
            let [collection] = await sql`SELECT ${sql(collectionColumns)} FROM collection WHERE collectionid = ${event.arguments.id}`;

            payload = { ...collection };

            if (event.info.selectionSetList.some((item) => item === "token")) {
                let tokenColumns = event.info.selectionSetList.filter((item) => item.startsWith("token/")).map((item) => item.split("/")[1]);
                let [token] = await sql`SELECT ${sql(tokenColumns)} FROM token WHERE collectionid = ${collection.id}`;

                payload.token = { ...token };
            }
            break
        case "Query que ha sido llamada":
            //Recoleccion de datos de la query a ejecutar
            //Un ejemplo de como se desarrolla y como se manda se encuentra en la parte superior
            //Para mas dudas checar este enlace: https://aws.amazon.com/es/blogs/mobile/appsync-graphql-sql-rds-proxy/
            break
    }

    await sql.end({ timeout: 0 });

    return payload;
}

module.exports = { handler };