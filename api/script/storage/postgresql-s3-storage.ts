import * as http from "http";
import * as q from "q";
import * as stream from "stream";
import * as pg from 'pg';
import * as storage from "./storage";
import { v4 as uuidv4 } from 'uuid';

import Promise = q.Promise;
import { isPrototypePollutionKey } from "./storage";
import { DeleteObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

export class PostgreSQLS3Storage implements storage.Storage {
  private postgreSqlPool: pg.Pool;
  private s3Client: S3Client;
  private _blobServerPromise: Promise<http.Server>;

  constructor() {
    this.postgreSqlPool = new pg.Pool({
      host: process.env.POSTGRESQL_HOST,
      port: Number(process.env.POSTGRESQL_PORT || 5432),
      user: process.env.POSTGRESQL_USERNAME,
      password: process.env.POSTGRESQL_PASSWORD,
      database: process.env.POSTGRESQL_DATABASE,
      max: Number(process.env.POSTGRESQL_MAX_POOL || 5),
      idleTimeoutMillis: Number(process.env.POSTGRESQL_POOL_IDLE_TIMEOUT_MILLIS || 30000),
      connectionTimeoutMillis: Number(process.env.POSTGRESQL_POOL_CONNECTION_TIMEOUT_MILLIS || 5),
    });

    this.s3Client = new S3Client({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS,
      },
    });
  }

  public checkHealth(): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.postgreSqlPool.query('SELECT 1');
        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public addAccount(account: storage.Account): Promise<string> {
    const deferred = q.defer<string>();

    (async () => {
      try {
        const email: string = account.email.toLowerCase();

        try {
          const isExist = await this.getAccountByEmail(email);

          if (isExist) {
            return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.AlreadyExists));
          }
        } catch (e) {
          if (e.code !== storage.ErrorCode.NotFound) {
            return deferred.reject(e);
          }
        }

        const queryText = `
          INSERT INTO accounts(id, name, email, "gitHubId")
          VALUES ($1, $2, $3, $4)
          RETURNING id;
        `;
        const queryValues = [uuidv4(), account.name, email, account.gitHubId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const id = res.rows[0].id;

        return deferred.resolve(id);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getAccount(accountId: string): Promise<storage.Account> {
    const deferred = q.defer<storage.Account>();

    (async () => {
      try {
        const queryText = 'SELECT * FROM accounts WHERE id=$1';
        const queryValues = [accountId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const account = res.rows[0];

        if (!account) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        return deferred.resolve(account);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getAccountByEmail(email: string): Promise<storage.Account> {
    const deferred = q.defer<storage.Account>();

    (async () => {
      try {
        const queryText = 'SELECT * FROM accounts WHERE email=$1';
        const queryValues = [email];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const account = res.rows[0];

        if (!account) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        return deferred.resolve(account);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public updateAccount(email: string, updates: storage.Account): Promise<void> {
    const deferred = q.defer<void>();

    if (!email) throw new Error("No account email");

    (async () => {
      try {
        const account = await this.getAccountByEmail(email);

        const queryText = `
          UPDATE accounts
          SET name=$1, "gitHubId"=$2
          WHERE id=$3
        `;
        const queryValues = [updates.name, updates.gitHubId, account.id];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getAccountIdFromAccessKey(accessKey: string): Promise<string> {
    const deferred = q.defer<string>();

    (async () => {
      try {
        const queryText = `
          SELECT b."accountId", b.expires FROM access_keys a
          JOIN access_key_to_account_map b
            ON b."accessKeyId"=a.id
          WHERE a.name=$1
        `;
        const queryValues = [accessKey];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const data = res.rows[0];

        if (!data) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        if (new Date().getTime() >= data.expires) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.Expired, "The access key has expired."));
        }

        return deferred.resolve(data.accountId);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public addApp(accountId: string, app: storage.App): Promise<storage.App> {
    const deferred = q.defer<storage.App>();

    (async () => {
      try {
        const account = await this.getAccount(accountId);

        const collaborators: storage.CollaboratorMap = {};
        collaborators[account.email] = <storage.CollaboratorProperties>{ accountId: accountId, permission: "Owner" };

        const queryText = `
          INSERT INTO apps(id, name, collaborators)
          VALUES ($1, $2, $3)
          RETURNING id;
        `;
        const queryValues = [uuidv4(), app.name, collaborators];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        const queryAccountToAppsText = `
          INSERT INTO account_to_apps_map("accountId", "appId")
          VALUES ($1, $2);
        `;
        const queryAccountToAppsValues = [account.id, res.rows[0].id];
        await this.postgreSqlPool.query(queryAccountToAppsText, queryAccountToAppsValues);

        app.collaborators = collaborators;
        app.id = res.rows[0].id;

        return deferred.resolve(app);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getApps(accountId: string): Promise<storage.App[]> {
    const deferred = q.defer<storage.App[]>();

    (async () => {
      try {
        const queryText = `
          SELECT b.*
          FROM account_to_apps_map a
          JOIN apps b
            ON b.id=a."appId"
          WHERE a."accountId"=$1
        `;
        const queryValues = [accountId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const apps = res.rows;

        apps.forEach((app: storage.App) => {
          this.addIsCurrentAccountProperty(app, accountId);
        });

        return deferred.resolve(apps);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getApp(accountId: string, appId: string): Promise<storage.App> {
    const deferred = q.defer<storage.App>();

    (async () => {
      try {
        const account = await this.getAccount(accountId);
        const queryText = 'SELECT * FROM apps WHERE id=$1';
        const queryValues = [appId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const app = res.rows[0];

        if (!app) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        this.addIsCurrentAccountProperty(app, account.id);

        return deferred.resolve(app);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  private getAppAccountId(appId: string): Promise<string> {
    const deferred = q.defer<string>();

    (async () => {
      try {
        const queryText = 'SELECT * FROM account_to_apps_map WHERE "appId"=$1';
        const queryValues = [appId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const data = res.rows[0];

        return deferred.resolve(data.accountId);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public removeApp(accountId: string, appId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        const appAccountId = await this.getAppAccountId(appId);

        if (accountId !== appAccountId) {
          throw new Error("Wrong accountId");
        }

        const queryText = 'DELETE FROM apps WHERE id=$1';
        const queryValues = [appId];
        await this.postgreSqlPool.query(queryText, queryValues);
        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public updateApp(accountId: string, app: storage.App, ensureIsOwner: boolean = true): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.getApp(accountId, app.id);
        this.removeIsCurrentAccountProperty(app);

        const queryText = 'UPDATE apps SET name=$1, collaborators=$2 WHERE id=$3';
        const queryValues = [app.name, app.collaborators, app.id];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public transferApp(accountId: string, appId: string, email: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        if (isPrototypePollutionKey(email)) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.Invalid, "Invalid email parameter"));
        }
        const app = await this.getApp(accountId, appId);
        const account = await this.getAccount(accountId);
        const requesterEmail = account.email;
        const targetOwnerAccount = await this.getAccountByEmail(email.toLowerCase());
        const targetOwnerAccountId = targetOwnerAccount.id;
        email = targetOwnerAccount.email;

        if (this.isOwner(app.collaborators, email)) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.AlreadyExists));
        }

        app.collaborators[requesterEmail].permission = storage.Permissions.Collaborator;
        if (this.isCollaborator(app.collaborators, email)) {
          app.collaborators[email].permission = storage.Permissions.Owner;
        } else {
          app.collaborators[email] = { permission: storage.Permissions.Owner, accountId: targetOwnerAccountId };
          this.addAppPointer(targetOwnerAccountId, app.id);
        }

        await this.updateApp(accountId, app);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public addCollaborator(accountId: string, appId: string, email: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        if (isPrototypePollutionKey(email)) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.Invalid, "Invalid email parameter"));
        }

        const app = await this.getApp(accountId, appId);

        if (this.isCollaborator(app.collaborators, email) || this.isOwner(app.collaborators, email)) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.AlreadyExists));
        }

        const targetCollaboratorAccount = await this.getAccountByEmail(email.toLowerCase());
        const targetCollaboratorAccountId = targetCollaboratorAccount.id;
        email = targetCollaboratorAccount.email;
        app.collaborators[email] = { accountId: targetCollaboratorAccountId, permission: storage.Permissions.Collaborator };
        await this.addAppPointer(targetCollaboratorAccountId, app.id);
        await this.updateApp(accountId, app);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getCollaborators(accountId: string, appId: string): Promise<storage.CollaboratorMap> {
    return this.getApp(accountId, appId).then((app: storage.App) => {
      return q<storage.CollaboratorMap>(app.collaborators);
    });
  }

  public removeCollaborator(accountId: string, appId: string, email: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        const app = await this.getApp(accountId, appId);

        if (this.isOwner(app.collaborators, email)) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound, "Cannot remove the owner of the app from collaborator list."));
        }

        const targetCollaboratorAccount = await this.getAccountByEmail(email.toLowerCase());
        const targetCollaboratorAccountId = targetCollaboratorAccount.id;

        if (!this.isCollaborator(app.collaborators, email) || !targetCollaboratorAccountId) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        await this.removeAppPointer(targetCollaboratorAccountId, appId);
        delete app.collaborators[email];
        await this.updateApp(accountId, app, /*ensureIsOwner*/ false);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public addDeployment(accountId: string, appId: string, deployment: storage.Deployment): Promise<string> {
    const deferred = q.defer<string>();

    (async () => {
      try {
        await this.getApp(accountId, appId);

        const queryText = `
            INSERT INTO deployments(id, name, "appId", key)
            VALUES ($1, $2, $3, $4)
            RETURNING id;
        `;
        const queryValues = [uuidv4(), deployment.name, appId, deployment.key];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve(res.rows[0].id);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getDeploymentInfo(deploymentKey: string): Promise<storage.DeploymentInfo> {
    const deferred = q.defer<storage.DeploymentInfo>();

    (async () => {
      const queryText = 'SELECT * FROM deployments WHERE key=$1';
      const queryValues = [deploymentKey];
      const res = await this.postgreSqlPool.query(queryText, queryValues);
      const deployment = res.rows[0];

      if (!deployment) {
        return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
      }

      const appId: string = deployment.package.appId;

      if (!appId) {
        return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
      }

      return deferred.resolve({ appId: appId, deploymentId: deployment.id });
    })();

    return deferred.promise;
  }

  public getPackageHistoryFromDeploymentKey(deploymentKey: string): Promise<storage.Package[]> {
    const deferred = q.defer<storage.Package[]>();

    (async () => {
      try {
        const queryText = `
          SELECT b.*
          FROM deployments a
          JOIN packages b
            ON b."deploymentId"=a.id
          WHERE a.key=$1
        `;
        const queryValues = [deploymentKey];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const packages = res.rows;

        return deferred.resolve(packages);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getDeployment(accountId: string, appId: string, deploymentId: string): Promise<storage.Deployment> {
    const deferred = q.defer<storage.Deployment>();

    (async () => {
      try {
        await this.getApp(accountId, appId);

        const queryText = `
          SELECT *
          FROM deployments a
          WHERE a.id=$1
        `;
        const queryValues = [deploymentId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const deployment = res.rows[0];

        if (!deployment) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        return deferred.resolve(deployment);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getDeployments(accountId: string, appId: string): Promise<storage.Deployment[]> {
    const deferred = q.defer<storage.Deployment[]>();

    (async () => {
      try {
        await this.getApp(accountId, appId);

        const queryText = `
          SELECT *
          FROM deployments a
          WHERE a."appId"=$1
        `;
        const queryValues = [appId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);
        const deployments = res.rows;

        return deferred.resolve(deployments);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public removeDeployment(accountId: string, appId: string, deploymentId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        const deployment = await this.getDeployment(accountId, appId, deploymentId);

        if (appId !== deployment.appId) {
          throw new Error("Wrong appId");
        }

        const queryText = `
          DELETE FROM deployments
          WHERE id=$1
        `;
        const queryValues = [deploymentId];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public updateDeployment(accountId: string, appId: string, deployment: storage.Deployment): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.getDeployment(accountId, appId, deployment.id);
        const queryText = 'UPDATE deployments SET name=$1, key=$2 WHERE id=$3';
        const queryValues = [deployment.name, deployment.key, deployment.id];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      }
      catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public commitPackage(accountId: string, appId: string, deploymentId: string, appPackage: storage.Package): Promise<storage.Package> {
    const deferred = q.defer<storage.Package>();

    (async () => {
      try {
        await this.getDeployment(accountId, appId, deploymentId);

        const queryText = `
          UPDATE packages SET rollout=NULL
          WHERE id=(SELECT id FROM packages WHERE "deploymentId"=$1 ORDER BY "uploadTime" DESC LIMIT 1);
        `;
        const queryValues = [deploymentId];
        await this.postgreSqlPool.query(queryText, queryValues);

        const queryPackageCountText = `
          SELECT COUNT(id) AS count
          FROM packages
          WHERE "deploymentId"=$1
        `;
        const queryPackageCountValues = [deploymentId];
        const res = await this.postgreSqlPool.query(queryPackageCountText, queryPackageCountValues);
        const count = res.rows[0].count || 0;
        appPackage.label = "v" + (Number(count) + 1);

        const queryPackageText = `
          INSERT INTO packages(id, "deploymentId", description, "isDisabled", "isMandatory", rollout, "appVersion", "packageHash", "blobUrl", size, "manifestBlobUrl", "releaseMethod", "uploadTime", label, "originalLabel", "originalDeployment")
          VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        `;

        const queryPackageValues = [
          uuidv4(),
          deploymentId,
          appPackage.description,
          !!appPackage.isDisabled,
          !!appPackage.isMandatory,
          appPackage.rollout,
          appPackage.appVersion,
          appPackage.packageHash,
          appPackage.blobUrl,
          appPackage.size,
          appPackage.manifestBlobUrl,
          appPackage.releaseMethod,
          new Date(appPackage.uploadTime),
          appPackage.label,
          appPackage.originalLabel,
          appPackage.originalDeployment,
        ];
        await this.postgreSqlPool.query(queryPackageText, queryPackageValues);

        return deferred.resolve(appPackage);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public clearPackageHistory(accountId: string, appId: string, deploymentId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.getDeployment(accountId, appId, deploymentId);

        const queryText = `
          DELETE from packages
          WHERE "deploymentId"=$1
        `;
        const queryValues = [deploymentId];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getPackageHistory(accountId: string, appId: string, deploymentId: string): Promise<storage.Package[]> {
    const deferred = q.defer<storage.Package[]>();

    (async () => {
      try {
        await this.getDeployment(accountId, appId, deploymentId);

        const queryText = `
          SELECT * FROM packages
          WHERE "deploymentId"=$1
        `;
        const queryValues = [deploymentId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve(res.rows);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public updatePackageHistory(accountId: string, appId: string, deploymentId: string, history: storage.Package[]): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        if (!history || !history.length) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.Invalid, "Cannot clear package history from an update operation"));
        }

        await this.getDeployment(accountId, appId, deploymentId);

        for (const h of history) {
          const queryPackageText = `
            UPDATE "packages" 
            SET "rollout" = $1, 
                "description" = $2, 
                "isDisabled" = $3, 
                "isMandatory" = $4, 
                "appVersion" = $5, 
                "packageHash" = $6, 
                "blobUrl" = $7, 
                "size" = $8, 
                "manifestBlobUrl" = $9, 
                "releaseMethod" = $10, 
                "uploadTime" = $11, 
                "label" = $12
            WHERE "id" = $13;
          `;
          const queryPackageValues = [
            h.rollout,
            h.description,
            h.isDisabled,
            h.isMandatory,
            h.appVersion,
            h.packageHash,
            h.blobUrl,
            h.size,
            h.manifestBlobUrl,
            h.releaseMethod,
            h.uploadTime,
            h.label,
            h.id
          ];
          await this.postgreSqlPool.query(queryPackageText, queryPackageValues);
        }

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public addBlob(blobId: string, stream: stream.Readable, streamLength: number): Promise<string> {
    const params = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: blobId,
      Body: stream,
      ContentLength: streamLength,
    };

    return q
      .Promise<string>((resolve, reject) => {
        this.s3Client
          .send(new PutObjectCommand(params))
          .then(() => {
            resolve(blobId);
          })
          .catch(reject);
      })
      .then(async () => {
        const queryText = `
          INSERT INTO blobs(key, url)
          VALUES ($1, $2)
          RETURNING key;
        `;
        const queryValues = [blobId, `https://${process.env.AWS_BUCKET_NAME}.s3.amazonaws.com/${blobId}`];
        await this.postgreSqlPool.query(queryText, queryValues);

        return blobId;
      });
  }

  public getBlobUrl(blobId: string): Promise<string> {
    return q.Promise<string>(async (resolve, reject) => {
      const queryText = `
        SELECT * FROM blobs
        WHERE key=$1;
      `;
      const queryValues = [blobId];
      const res = await this.postgreSqlPool.query(queryText, queryValues);

      const blobPath = res.rows[0]?.url;
      if (blobPath) {
        resolve(blobPath);
      } else {
        reject(new Error("Blob not found"));
      }
    });
  }

  public removeBlob(blobId: string): Promise<void> {
    const params = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: blobId,
    };

    return q
      .Promise<void>((resolve, reject) => {
        this.s3Client
          .send(new DeleteObjectCommand(params))
          .then(() => {
            resolve();
          })
          .catch(reject);
      })
      .then(async () => {
        const queryText = `
          DELETE FROM blobs
          WHERE key=$1;
        `;
        const queryValues = [blobId];
        await this.postgreSqlPool.query(queryText, queryValues);

        return q(<void>null);
      });
  }

  public addAccessKey(accountId: string, accessKey: storage.AccessKey): Promise<string> {
    const deferred = q.defer<string>();

    (async () => {
      try {
        const account = await this.getAccount(accountId);

        const queryText = `
          INSERT INTO access_keys(id, name, "createdBy", description, expires, "friendlyName", "isSession")
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          RETURNING id;
        `;
        const queryValues = [uuidv4(), accessKey.name, account.email, accessKey.description, new Date(accessKey.expires), accessKey.friendlyName, !!accessKey.isSession];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        const queryMapText = `
          INSERT INTO access_key_to_account_map("accessKeyId", "accountId", expires)
          VALUES ($1, $2, $3)
          RETURNING "accessKeyId";
        `;
        const queryMapValues = [res.rows[0].id, accountId, new Date(accessKey.expires)];
        await this.postgreSqlPool.query(queryMapText, queryMapValues);

        return deferred.resolve(res.rows[0].id);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getAccessKey(accountId: string, accessKeyId: string): Promise<storage.AccessKey> {
    const deferred = q.defer<storage.AccessKey>();

    (async () => {
      try {
        const queryText = `
          SELECT a.*, b."accountId"
          FROM access_keys a
          JOIN access_key_to_account_map b
            ON b."accessKeyId"=a.id
          WHERE a.id=$1
        `;
        const queryValues = [accessKeyId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        const expectedAccountId = res.rows[0]?.accountId;

        if (!expectedAccountId || expectedAccountId !== accountId) {
          return deferred.reject(PostgreSQLS3Storage.getError(storage.ErrorCode.NotFound));
        }

        return deferred.resolve(res.rows[0]);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public getAccessKeys(accountId: string): Promise<storage.AccessKey[]> {
    const deferred = q.defer<storage.AccessKey[]>();

    (async () => {
      try {
        const queryText = `
          SELECT b.*
          FROM access_key_to_account_map a
          JOIN access_keys b
            ON b.id=a."accessKeyId"
          WHERE a."accountId"=$1
        `;
        const queryValues = [accountId];
        const res = await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve(res.rows);
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public removeAccessKey(accountId: string, accessKeyId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.getAccessKey(accountId, accessKeyId);

        const queryText = `
          DELETE FROM access_keys
          WHERE id=$1;
        `;
        const queryValues = [accessKeyId];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public updateAccessKey(accountId: string, accessKey: storage.AccessKey): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        await this.getAccessKey(accountId, accessKey.id);

        const queryText = `
          UPDATE access_keys
          SET name=$1, description=$2, expires=$3, "friendlyName"=$4, "isSession"=$5
          WHERE id=$6;
        `;
        const queryValues = [accessKey.name, accessKey.description, new Date(accessKey.expires), accessKey.friendlyName, !!accessKey.isSession, accessKey.id];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  public dropAll(): Promise<void> {
    if (this._blobServerPromise) {
      return this._blobServerPromise.then((server: http.Server) => {
        const deferred: q.Deferred<void> = q.defer<void>();
        server.close((err?: Error) => {
          if (err) {
            deferred.reject(err);
          } else {
            deferred.resolve();
          }
        });
        return deferred.promise;
      });
    }

    return q(<void>null);
  }

  private addIsCurrentAccountProperty(app: storage.App, accountId: string): void {
    if (app && app.collaborators) {
      Object.keys(app.collaborators).forEach((email: string) => {
        if (app.collaborators[email].accountId === accountId) {
          app.collaborators[email].isCurrentAccount = true;
        }
      });
    }
  }

  private removeIsCurrentAccountProperty(app: storage.App): void {
    if (app && app.collaborators) {
      Object.keys(app.collaborators).forEach((email: string) => {
        if (app.collaborators[email].isCurrentAccount) {
          delete app.collaborators[email].isCurrentAccount;
        }
      });
    }
  }

  private isOwner(list: storage.CollaboratorMap, email: string): boolean {
    return list && list[email] && list[email].permission === storage.Permissions.Owner;
  }

  private isCollaborator(list: storage.CollaboratorMap, email: string): boolean {
    return list && list[email] && list[email].permission === storage.Permissions.Collaborator;
  }

  private isAccountIdCollaborator(list: storage.CollaboratorMap, accountId: string): boolean {
    const keys: string[] = Object.keys(list);
    for (let i = 0; i < keys.length; i++) {
      if (list[keys[i]].accountId === accountId) {
        return true;
      }
    }

    return false;
  }

  private removeAppPointer(accountId: string, appId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        const queryText = `
          DELETE FROM account_to_apps_map
          WHERE "accountId" = $1 AND "appId" = $2;
        `;
        const queryValues = [accountId, appId];
        await this.postgreSqlPool.query(queryText, queryValues);
        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  private addAppPointer(accountId: string, appId: string): Promise<void> {
    const deferred = q.defer<void>();

    (async () => {
      try {
        const queryText = `
          INSERT INTO account_to_apps_map("accountId", "appId")
          VALUES ($1, $2);
        `;
        const queryValues = [accountId, appId];
        await this.postgreSqlPool.query(queryText, queryValues);

        return deferred.resolve();
      } catch (e) {
        return deferred.reject(e);
      }
    })();

    return deferred.promise;
  }

  private static getError(errorCode: storage.ErrorCode, message?: string): any {
    return storage.storageError(errorCode, message);
  }
}
