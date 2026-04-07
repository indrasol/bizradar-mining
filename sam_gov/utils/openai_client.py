from sam_gov.config.settings import AZURE_OPENAI_API_KEY as azure_api_key
from sam_gov.config.settings import AZURE_OPENAI_BASE_URL as azure_base_url
from sam_gov.config.settings import AZURE_OPENAI_DEPLOYMENT as azure_deployment
from sam_gov.config.settings import AZURE_OPENAI_EMBEDDING_MODEL as azure_embedding_model
from sam_gov.utils.logger import get_logger
from openai import AzureOpenAI

logger = get_logger(__name__)


def get_openai_client():
    try:
        if not azure_api_key or not azure_base_url:
            logger.error("CRITICAL: Azure OpenAI configuration is missing (AZURE_OPENAI_API_KEY_BIZ or AZURE_OPENAI_BASE_URL_BIZ)")
            return None
        endpoint = azure_base_url.split("/openai/v1/")[0]
        logger.info(f"Initializing Azure OpenAI client with endpoint: {endpoint}")
        client = AzureOpenAI(
            api_key=azure_api_key,
            api_version="2024-02-01",
            azure_endpoint=endpoint
        )
        logger.info("Azure OpenAI client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Azure OpenAI client: {str(e)}")
    return None