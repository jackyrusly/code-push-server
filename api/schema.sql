CREATE TABLE "accounts" (
    "id" UUID PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "email" VARCHAR(255) UNIQUE NOT NULL,
    "gitHubId" VARCHAR(255) NOT NULL,
    "createdTime" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE "apps" (
    "id" UUID PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "createdTime" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "collaborators" JSONB
);

CREATE TABLE "deployments" (
    "id" UUID PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "appId" UUID REFERENCES "apps"("id") ON DELETE CASCADE,
    "createdTime" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "key" VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE "packages" (
    "id" UUID PRIMARY KEY,
    "deploymentId" UUID REFERENCES "deployments"("id") ON DELETE CASCADE,
    "description" TEXT,
    "isDisabled" BOOLEAN NOT NULL DEFAULT FALSE,
    "isMandatory" BOOLEAN NOT NULL DEFAULT FALSE,
    "rollout" INT,
    "appVersion" VARCHAR(255) NOT NULL,
    "packageHash" VARCHAR(255) NOT NULL,
    "blobUrl" VARCHAR(255),
    "size" INT NOT NULL,
    "manifestBlobUrl" VARCHAR(255),
    "releaseMethod" VARCHAR(255),
    "uploadTime" TIMESTAMPTZ NOT NULL,
    "label" VARCHAR(255)
    "originalLabel" VARCHAR(255)
    "originalDeployment" VARCHAR(255)
);

CREATE TABLE "blobs" (
    "key" VARCHAR(255) PRIMARY KEY,
    "url" VARCHAR(255) NOT NULL
);

CREATE TABLE "access_keys" (
    "id" UUID PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "createdTime" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "createdBy" VARCHAR(255),
    "description" TEXT,
    "expires" TIMESTAMPTZ,
    "friendlyName" VARCHAR(255),
    "isSession" BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE "access_key_to_account_map" (
    "accessKeyId" UUID REFERENCES "access_keys"("id") ON DELETE CASCADE,
    "accountId" UUID REFERENCES "accounts"("id") ON DELETE CASCADE,
    "expires" TIMESTAMPTZ,
    PRIMARY KEY ("accessKeyId", "accountId")
);

CREATE TABLE "account_to_apps_map" (
    "accountId" UUID REFERENCES "accounts"("id") ON DELETE CASCADE,
    "appId" UUID REFERENCES "apps"("id") ON DELETE CASCADE,
    PRIMARY KEY ("accountId", "appId")
);
