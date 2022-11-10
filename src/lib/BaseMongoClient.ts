import * as migrateMongo from 'migrate-mongo';
import { ClientSession, Db, MongoClient, MongoClientOptions } from 'mongodb';

let _dbInstance: Db | undefined;
let _client: MongoClient | undefined;

export class BaseMongoClient {
  private _dbUri: string;
  private _dbName: string;
  private _options: MongoClientOptions;

  constructor(mongoConfig: IMongoConfig, poolSize = 10) {
    this._dbUri = mongoConfig.uri;
    this._dbName = mongoConfig.dbName;
    this._options = {
      minPoolSize: poolSize,
      tls: mongoConfig.ssl,
      connectTimeoutMS: 10000,
      socketTimeoutMS: 10000,
    };
  }

  public async getDb(): Promise<Db | undefined> {
    await this.openDbConnection();
    return _dbInstance;
  }

  public async performMigrations(): Promise<any> {
    try {
      const migrationConfig = {
        mongodb: {
          url: this._dbUri,
          databaseName: this._dbName,
          options: {
            useNewUrlParser: true,
            useUnifiedTopology: true,
          },
        },
        migrationsDir: 'migrations',
        changelogCollectionName: 'changelog',
      };

      migrateMongo.config.set(migrationConfig);
      await migrateMongo.config.shouldExist();

      const { db, client } = await migrateMongo.database.connect();
      const migrated = await migrateMongo.up(db, client);
      await client.close();
      return migrated;
    } catch (error) {
      console.log('Error occured while performing migrations', error);
      throw error;
    }
  }

  public async openDbConnection(): Promise<void> {
    if (_client && _dbInstance) {
      return;
    }

    try {
      const client = new MongoClient(this._dbUri, { ...this._options });
      await client.connect();
      await client.db('admin').command({ ping: 1 });

      _dbInstance = client.db(this._dbName);
      _client = client;
    } catch (err) {
      _dbInstance = undefined;
      console.log('Error occured while connecting to db', err);
      throw err;
    }
  }

  public async closeDbConnection(): Promise<void> {
    if (!_client) {
      return;
    }

    try {
      await _client.close();
      _dbInstance = undefined;
      _client = undefined;
    } catch (err) {
      console.log('Error occured while disconnecting from db', err);
      throw err;
    }
  }

  public async ping(): Promise<void> {
    await this.getDb();
    if (!_client) {
      throw new Error('Client missing or not configured');
    }
    await _client.db('admin').command({ ping: 1 });
  }

  public async getSession(): Promise<ClientSession | undefined> {
    await this.openDbConnection();
    return _client?.startSession();
  }
}

export interface IMongoConfig {
  uri: string;
  dbName: string;
  ssl: boolean;
  logLevel: string;
}
