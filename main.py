import asyncio
from genericpath import exists
import json
import os
import re
from sqlite3 import Timestamp
from typing import final
import psycopg2
from configparser import ConfigParser

from near_lake_framework import near_primitives, LakeConfig, streamer

def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db={}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]]= param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

def init_db():
    commands =( 
        """
        CREATE TABLE IF NOT EXISTS Collection (
            collectionID    bigint          NOT NULL,
            owner_id        text            NOT NULL,
            title           text            NOT NULL,
            description     text            NOT NULL,
            tokenCount      bigint          NOT NULL,
            salesCount      bigint          NOT NULL,
            saleVolume      money           NOT NULL,
            mediaIcon       text            NOT NULL,
            mediaBanner     text            NOT NULL,
            date            text            NOT NULL,
            visibility      boolean         NOT NULL,
            PRIMARY KEY (collectionID)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS profile (
            username        text        NOT NULL,
            media           text        NOT NULL,
            biography       text        NOT NULL,
            tokCreated      bigint      NOT NULL,
            tokBought       bigint      NOT NULL,
            socialMedia     text        NOT NULL,
            date            text        NOT NULL,
            PRIMARY KEY (username)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS token (
            tokenId         bigint      NOT NULL,
            contract        text        NOT NULL,
            owner_id        text        NOT NULL,
            title           text        NOT NULL,
            description     text        NOT NULL,
            media           text        NOT NULL,
            creator         text        NOT NULL,
            price           text        NOT NULL,
            onSale          boolean     NOT NULL,
            extra           text        NOT NULL,
            approvalID      bigint      NOT NULL,
            collectionID    bigint      NOT NULL,
            date            text        NOT NULL,
            PRIMARY KEY (tokenId)
        )
        """,
        )
    
    conn = None
    try:
        params = config()
        print('Conectando a la base de datos')
        conn = psycopg2.connect(**params)
        cur=conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Tablas creadas.')


def execute_insert(query):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print(query)
        cur.execute(query)
        conn.commit()
        cur.close()
        print('Log Insertado')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


async def handle_streamer_message(streamer_message: near_primitives.StreamerMessage):
    for shard in streamer_message.shards:
        for receipt_execution_outcome in shard.receipt_execution_outcomes:
            if receipt_execution_outcome.receipt.receiver_id == "v4.nativo-market.testnet":
                for log in receipt_execution_outcome.execution_outcome.outcome.logs:
                    method=""
                    if "visibility" in log : 
                        if "create" in log :
                            method="add_new_user_collection-create"
                            data = json.loads(log)['params']
                            if 'visibility' not in data:
                                data['visibility'] = 'true'
                            query = "INSERT INTO collection(collectionid,date,description,mediabanner,mediaicon,owner_id,salescount,salevolume,title,tokencount,visibility) VALUES ("+str(data['collection_id'])+",'"+str(streamer_message.block.header.timestamp_nanosec)+"','"+str(data['description'])+"','"+str(data['media_banner'])+"','"+str(data['media_icon'])+"','"+str(data['owner_id'])+"',"+str(0)+","+str(0)+",'"+str(data['title'])+"',"+str(0)+","+str(data['visibility'])+")"
                            execute_insert(query)
                        else:
                            method="add_new_user_collection-edit"
                            data = json.loads(log)['params']
                            query = "UPDATE collection SET description='"+str(data['description'])+"',mediabanner='"+str(data['media_banner'])+"',mediaicon='"+str(data['media_icon'])+"',title='"+str(data['title'])+"',visibility="+str(data['visibility'])+" WHERE collectionid="+str(data['collection_id'])
                            execute_insert(query)
                    if "approval_id" in log :
                        method="add_token_to_collection"
                        data = json.loads(log)['params']
                        queryInsert= "INSERT INTO token(approvalid,collectionid,contract,creator,date,description,extra,media,onsale,owner_id,price,title,tokenid) VALUES ("+str(data['approval_id'])+","+str(data['collection_id'])+",'"+str(data['contract_id'])+"','"+str(data['creator'])+"','"+str(streamer_message.block.header.timestamp_nanosec)+"','"+str(data['description'])+"','','"+str(data['media'])+"',false,'"+str(data['owner_id'])+"','0','"+str(data['title'])+"',"+str(data['token_id'])+")"
                        queryUpTokCol = "UPDATE collection SET tokencount=tokencount+1 WHERE collectionID="+str(data['collection_id'])
                        queryUpTokCrea = "UPDATE profile SET tokcreated=tokcreated+1 WHERE username='"+str(data['creator'])+"'"
                        execute_insert(queryInsert)
                        execute_insert(queryUpTokCol)
                        execute_insert(queryUpTokCrea)
                    if "username" in log :
                        if "create" in log :
                            method="add_new_profile-create"
                            data = json.loads(log)['params']
                            query = "INSERT INTO profile(biography,date,media,socialmedia,tokbought,tokcreated,username) VALUES ('"+str(data['biography'])+"','"+str(streamer_message.block.header.timestamp_nanosec)+"','"+str(data['media'])+"','"+str(data['social_media'])+"',0,0,'"+str(data['username'])+"')"
                            execute_insert(query)
                        else:
                            method="add_new_profile-edit"
                            data = json.loads(log)['params']
                            query = "UPDATE profile SET media='"+str(data['media'])+"',biography='"+str(data['biography'])+"',socialMedia='"+str(data['social_media'])+"' WHERE username='"+str(data['username'])+"'"
                            execute_insert(query)
                    # output = {
                    #     "receipt_id": receipt_execution_outcome.receipt.receipt_id,
                    #     "data": json.loads(log),
                    #     "contract": receipt_execution_outcome.receipt.receiver_id,
                    #     "method": method
                    # }
                    # receipt = receipt_execution_outcome.receipt.receipt_id
                    # data = json.loads(log)
                    # contract = receipt_execution_outcome.receipt.receiver_id
                    # date = streamer_message.block.header.timestamp_nanosec
                    # insert_data(receipt,data,contract,method,date)
                    # print(json.dumps(output, indent=4))
            else:
                continue

                


async def main():
    init_db()
    config = LakeConfig.testnet()
    config.start_block_height = 97336124
    config.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    config.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    stream_handle, streamer_messages_queue = streamer(config)
    while True:
        streamer_message = await streamer_messages_queue.get()
        await handle_streamer_message(streamer_message)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
