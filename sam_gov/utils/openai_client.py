from sam_gov.config.settings import OPENAI_API_KEY as api_key
from sam_gov.config.settings import AZURE_OPENAI_API_KEY as azure_api_key
from sam_gov.config.settings import AZURE_OPENAI_BASE_URL as azure_base_url
from sam_gov.config.settings import AZURE_OPENAI_DEPLOYMENT as azure_deployment
from sam_gov.config.settings import AZURE_OPENAI_EMBEDDING_MODEL as azure_embedding_model
from sam_gov.utils.logger import get_logger
from openai import OpenAI

logger = get_logger(__name__)


def get_openai_client():
    try:
        if not api_key:
            logger.warning("OPENAI_API_KEY environment variable not set")
        else:
            logger.info("OPENAI_API_KEY environment variable found")
            client = OpenAI(api_key=azure_api_key, base_url=azure_base_url)
            logger.info("Azure OpenAI client initialized successfully")
            return client
    except Exception as e:
        logger.error(f"Failed to initialize Azure OpenAI client: {str(e)}")
    return None