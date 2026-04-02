import os
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent.parent


def _first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip() != "":
                return value
            continue
        return value
    return None


def env_str(name: str, default: str = "", legacy_names: tuple[str, ...] = ()) -> str:
    candidates = [os.getenv(name)]
    for legacy_name in legacy_names:
        candidates.append(os.getenv(legacy_name))
    resolved = _first_non_empty(*candidates)
    if resolved is None:
        return default
    return str(resolved).strip()


def env_int(name: str, default: int, legacy_names: tuple[str, ...] = ()) -> int:
    raw = env_str(name, "", legacy_names=legacy_names)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float, legacy_names: tuple[str, ...] = ()) -> float:
    raw = env_str(name, "", legacy_names=legacy_names)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_bool(name: str, default: bool = False, legacy_names: tuple[str, ...] = ()) -> bool:
    raw = env_str(name, "", legacy_names=legacy_names)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


# Environment selection must happen before loading .env files.
ENV_BIZ = env_str("ENV_BIZ", "development")


def load_env_file() -> bool:
    env_file = BASE_DIR / f".env.{ENV_BIZ}"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=True)
        return True
    default_env_file = BASE_DIR / ".env"
    if default_env_file.exists():
        load_dotenv(dotenv_path=default_env_file, override=True)
        return True
    return False


load_env_file()


ENV = ENV_BIZ
title = env_str("title_BIZ")
description = env_str("description_BIZ")
version = env_str("version_BIZ")

SAM_API_KEY = env_str("SAM_API_KEY_BIZ", legacy_names=("SAMAPIKEY",))

DB_HOST = env_str("DB_HOST_BIZ", legacy_names=("DBHOSTBIZ",))
DB_PORT = env_str("DB_PORT_BIZ", legacy_names=("DBPORTBIZ",))
DB_NAME = env_str("DB_NAME_BIZ", legacy_names=("DBNAMEBIZ",))
DB_USER = env_str("DB_USER_BIZ", legacy_names=("DBUSERBIZ",))
DB_PASSWORD = env_str("DB_PASSWORD_BIZ", legacy_names=("DBPASSWORDBIZ",))

OPENAI_API_KEY = env_str("OPENAI_API_KEY_BIZ", legacy_names=("OPENAIAPIKEYBIZ",))

PINECONE_API_KEY = env_str("PINECONE_API_KEY_BIZ", legacy_names=("PINECONEAPIKEYBIZ",))
PINECONE_ENV = env_str("PINECONE_ENV_BIZ", legacy_names=("PINECONEENVBIZ",))
PINECONE_INDEX_NAME = env_str("PINECONE_INDEX_NAME_BIZ", legacy_names=("PINECONEINDEXNAMEBIZ",))
EMBEDDING_MODEL = env_str("EMBEDDING_MODEL_BIZ", legacy_names=("EMBEDDINGMODELBIZ",))

GITHUB_TOKEN = env_str("GITHUB_TOKEN_BIZ", legacy_names=("GITHUB_TOKEN",))
GITHUB_REPO = env_str("GITHUB_REPO_BIZ", legacy_names=("GITHUB_REPO",))
GITHUB_OWNER = env_str("GITHUB_OWNER_BIZ", legacy_names=("GITHUB_OWNER",))

JWT_SECRET = env_str(
    "SUPABASE_JWT_SECRET_BIZ",
    env_str("JWT_SECRET_BIZ", "your-supabase-jwt-secret", legacy_names=("JWT_SECRET",)),
)
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

SUPABASE_JWT_SECRET_BIZ = env_str("SUPABASE_JWT_SECRET_BIZ")
SUPABASE_URL = env_str("SUPABASE_URL_BIZ")
SUPABASE_ANON_KEY = env_str("SUPABASE_ANON_KEY_BIZ")
SUPABASE_SERVICE_KEY = env_str("SUPABASE_SERVICE_KEY_BIZ")

STRIPE_SECRET_KEY = env_str("STRIPE_SECRET_KEY_BIZ", legacy_names=("STRIPE_SECRET_KEY",))
STRIPE_PUBLISHABLE_KEY = env_str("STRIPE_PUBLISHABLE_KEY_BIZ", legacy_names=("STRIPE_PUBLISHABLE_KEY",))
STRIPE_WEBHOOK_SECRET = env_str("STRIPE_WEBHOOK_SECRET_BIZ", legacy_names=("STRIPE_WEBHOOK_SECRET",))
REDIRECT_URL = env_str("REDIRECT_URL_BIZ", "http://localhost:8080")
SEARCH_RESULT_LIMIT = env_int("SEARCH_RESULT_LIMIT_BIZ", 50)
PRO_SEARCH_RESULT_LIMIT = env_int("PRO_SEARCH_RESULT_LIMIT_BIZ", 100)

REDIS_HOST = env_str("REDIS_HOST_BIZ", legacy_names=("REDISHOSTBIZ",))
REDIS_PORT = env_str("REDIS_PORT_BIZ", legacy_names=("REDISPORTBIZ",))
REDIS_USERNAME = env_str("REDIS_USERNAME_BIZ", legacy_names=("REDISUSERNAMEBIZ",))
REDIS_PASSWORD = env_str("REDIS_PASSWORD_BIZ", legacy_names=("REDISPASSWORDBIZ",))

IMPORT_USER = env_str("IMPORT_USER_BIZ", legacy_names=("IMPORTUSERBIZ",))
SENDGRID_API_KEY = env_str("SENDGRID_API_KEY_BIZ", legacy_names=("SENDGRIDAPIKEYBIZ",))

TRIAL_DURATION_MINUTES = env_int(
    "TRIAL_DURATION_MINUTES_BIZ",
    5 if ENV_BIZ == "development" else 21600,
    legacy_names=("TRIAL_DURATION_MINUTES",),
)

# Azure infra/deploy naming (3-container pipeline)
AZ_SUBSCRIPTION_ID_BIZ = env_str("AZ_SUBSCRIPTION_ID_BIZ", legacy_names=("AZ_SUBSCRIPTION_ID",))
AZ_RESOURCE_GROUP_BIZ = env_str("AZ_RESOURCE_GROUP_BIZ", legacy_names=("AZ_RESOURCE_GROUP",))
AZ_LOCATION_BIZ = env_str("AZ_LOCATION_BIZ", legacy_names=("AZ_LOCATION",))
AZ_SERVICEBUS_NAMESPACE_BIZ = env_str("AZ_SERVICEBUS_NAMESPACE_BIZ", legacy_names=("AZ_SERVICEBUS_NAMESPACE",))
AZ_STORAGE_ACCOUNT_BIZ = env_str("AZ_STORAGE_ACCOUNT_BIZ", legacy_names=("AZ_STORAGE_ACCOUNT",))
AZ_STORAGE_CONTAINER_BIZ = env_str("AZ_STORAGE_CONTAINER_BIZ", legacy_names=("AZ_STORAGE_CONTAINER",))
AZ_CONTAINERAPPS_ENV_BIZ = env_str("AZ_CONTAINERAPPS_ENV_BIZ", legacy_names=("AZ_CONTAINERAPPS_ENV",))
AZ_LOG_ANALYTICS_WS_BIZ = env_str("AZ_LOG_ANALYTICS_WS_BIZ", legacy_names=("AZ_LOG_ANALYTICS_WS",))
AZ_APP_INSIGHTS_NAME_BIZ = env_str("AZ_APP_INSIGHTS_NAME_BIZ", legacy_names=("AZ_APP_INSIGHTS_NAME",))
AZ_WORKER_IDENTITY_NAME_BIZ = env_str("AZ_WORKER_IDENTITY_NAME_BIZ", legacy_names=("AZ_WORKER_IDENTITY_NAME",))
AZ_WORKER_APP_PREFIX_BIZ = env_str("AZ_WORKER_APP_PREFIX_BIZ", legacy_names=("AZ_WORKER_APP_PREFIX",))
AZ_KEYVAULT_NAME_BIZ = env_str("AZ_KEYVAULT_NAME_BIZ", legacy_names=("AZ_KEYVAULT_NAME",))
AZ_ACR_NAME_BIZ = env_str("AZ_ACR_NAME_BIZ", legacy_names=("AZ_ACR_NAME",))
AZ_CONTAINER_IMAGE_BIZ = env_str("AZ_CONTAINER_IMAGE_BIZ", legacy_names=("AZ_CONTAINER_IMAGE",))
AZ_CORE_APP_MIN_REPLICAS_BIZ = env_int(
    "AZ_CORE_APP_MIN_REPLICAS_BIZ",
    1,
    legacy_names=("AZ_CORE_APP_MIN_REPLICAS",),
)
AZ_CORE_APP_MAX_REPLICAS_BIZ = env_int(
    "AZ_CORE_APP_MAX_REPLICAS_BIZ",
    10,
    legacy_names=("AZ_CORE_APP_MAX_REPLICAS",),
)
AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ = env_int(
    "AZ_CLASSIFY_APP_MIN_REPLICAS_BIZ",
    0,
    legacy_names=("AZ_CLASSIFY_APP_MIN_REPLICAS",),
)
AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ = env_int(
    "AZ_CLASSIFY_APP_MAX_REPLICAS_BIZ",
    6,
    legacy_names=("AZ_CLASSIFY_APP_MAX_REPLICAS",),
)
AZ_EMBED_APP_MIN_REPLICAS_BIZ = env_int(
    "AZ_EMBED_APP_MIN_REPLICAS_BIZ",
    0,
    legacy_names=("AZ_EMBED_APP_MIN_REPLICAS",),
)
AZ_EMBED_APP_MAX_REPLICAS_BIZ = env_int(
    "AZ_EMBED_APP_MAX_REPLICAS_BIZ",
    4,
    legacy_names=("AZ_EMBED_APP_MAX_REPLICAS",),
)

AZURE_OPENAI_API_KEY = env_str("AZURE_OPENAI_API_KEY_BIZ")
AZURE_OPENAI_BASE_URL = env_str("AZURE_OPENAI_BASE_URL_BIZ")
AZURE_OPENAI_DEPLOYMENT = env_str("AZURE_OPENAI_DEPLOYMENT_BIZ")
AZURE_OPENAI_EMBEDDING_MODEL = env_str("AZURE_OPENAI_EMBEDDING_MODEL_BIZ")


def get_stripe_secret_key() -> str:
    return STRIPE_SECRET_KEY

