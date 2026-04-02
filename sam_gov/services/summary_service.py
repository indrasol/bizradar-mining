import asyncio
import os
from sam_gov.utils.db_utils import get_db_connection
from sam_gov.utils.logger import get_logger
from sam_gov.utils.openai_client import get_openai_client
import aiohttp
from typing import List, Dict, Any
import psycopg2
from datetime import datetime, date

import json

# Configure logging
logger = get_logger(__name__)


def normalize_thread_key(solicitation_number: str = None, notice_id: str = None) -> str:
    """Create a stable thread key shared across importer and summary logic."""
    sol = str(solicitation_number or "").strip()
    nid = str(notice_id or "").strip()
    return sol or nid


def _empty_summary_response() -> Dict[str, Dict[str, str]]:
    """Return a consistent summary payload shape for callers."""
    return {
        "summary": {
            "sponsor": "Not specified",
            "objective": "",
            "goal": "",
            "eligibility": "",
            "key_facts": "",
            "due_date": "",
            "budget": "",
        }
    }


def normalize_bulleted_summary(summary: dict) -> str:
    """
    Normalize a summary into a structured format with specific fields.
    If normalization fails, return DEFAULT_SUMMARY.
    """
    try:
        if not summary or not isinstance(summary, dict):
            return normalize_bulleted_summary(DEFAULT_SUMMARY)
        headers = ["Sponsor", "Objective", "Goal", "Eligibility", "Key Facts", "Contact information", "Due Date"]
        head_keys = ["sponsor", "objective", "goal", "eligibility", "key_facts", "contact_info", "due_date"]
        bullets = []
        for header, key in zip(headers, head_keys):
            if key in summary and "Not specified" not in summary[key]:
                bullets.append(f"*   **{header}**: {summary[key]}")
        return "\n".join(bullets) if bullets else normalize_bulleted_summary(DEFAULT_SUMMARY)
    except Exception:
        return normalize_bulleted_summary(DEFAULT_SUMMARY)

async def fetch_description_from_sam(description_url):
    """
    Fetches the description from SAM.gov API.
    
    Args:
        description_url (str): The URL to fetch the description from
        
    Returns:
        str: The description text or None if failed
    """
    try:
        # Get SAM.gov API key from environment variable
        # api_key = os.getenv("SAM_API_KEY")
        from sam_gov.config.settings import SAM_API_KEY as api_key
        if not api_key:
            logger.error("SAM.gov API key not found in environment variables")
            return None

        # Add API key to URL
        separator = "&" if "?" in description_url else "?"
        url_with_key = f"{description_url}{separator}api_key={api_key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url_with_key) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('description', '')
                elif response.status == 429:
                    logger.error("Rate limit exceeded. Sleeping for 1 hour.")
                    await asyncio.sleep(3600)
                    return await fetch_description_from_sam(description_url)
                else:
                    logger.error(f"Failed to fetch description from SAM.gov: {response.status}")
                    return ""
    except Exception as e:
        logger.error(f"Error fetching description from SAM.gov: {str(e)}")
        return ""

SUMMARY_TEMPLATE = """
   "summary":{
  "sponsor": "Full precise complete name of the sponsoring organization",
  "objective": "Main purpose or objective of the opportunity in 1 sentence",
  "goal": "Primary goal or intended outcome in 1-2 sentences",
  "eligibility": "Eligibility criteria for applicants",
  "key_facts": "Important details like budget, timeline, or special requirements missed out in other fields",
  "due_date": "Application or submission deadline in YYYY-MM-DD format",
  "budget": "Estimated budget for the opportunity"
}
"""

async def generate_description_summary(description_text, max_length=300):
    """
    Generates a clear, engaging summary of a contract description.
    
    Args:
        description_text (str): The original contract description text
        max_length (int): Maximum token length for the summary
        
    Returns:
        str: A clear, concise summary capturing key essentials
    """
    try:
        if not description_text or description_text.strip() == "":
            return _empty_summary_response()
            
        # Truncate very long descriptions
        if len(description_text) > 6000:
            description_text = description_text[:6000] + "..."

        client = get_openai_client()   
        logger.info("OpenAI client initialized successfully")
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            response_format={ "type": "json_object" },
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert government contract analyst. Write a concise summary for a business audience.\n"
                    "Please analyze this opportunity and return a JSON object with the following structure:\n"
                    "{\n"
                    "   " + SUMMARY_TEMPLATE + "\n}\n"
                    "If any information is not available in the description, use 'Not specified' as the value.\n"
                    "For contact information and due date, leave as empty string if not specified."
                )
                },
                {
                    "role": "user",
                    "content": (
                        "Summarize this government contract opportunity for a business audience:\n\n"
                        f"{description_text}"
                    )
                }
            ],
            temperature=0.2, 
            max_tokens=max_length,
            n=1
        )
        logger.info("OpenAI response received")
        summary = response.choices[0].message.content.strip()
        
        # Ensure bullet list formatting
        # summary = normalize_bulleted_summary(summary)
        
        # logger.info(f"Generated concise summary of {len(summary)} chars")
        parsed_summary = json.loads(summary)
        if not isinstance(parsed_summary, dict):
            return _empty_summary_response()
        if "summary" not in parsed_summary or not isinstance(parsed_summary.get("summary"), dict):
            return _empty_summary_response()
        return parsed_summary
        
    except Exception as e:
        logger.error(f"Summary generation error: {str(e)}")
        return _empty_summary_response()

async def generate_title_and_summary(opportunity_title, description_text, max_length=400):
    """
    Generates an improved title (based on the original title and description) and a summary for a contract opportunity.
    Returns a dict: {"title": ..., "summary": ...}
    """
    try:
        if not description_text or description_text.strip() == "":
            return {
                "title": opportunity_title or "Untitled Opportunity",
                "summary": DEFAULT_SUMMARY
            }
            
        # Truncate very long descriptions to avoid excessive token usage
        truncated_desc = description_text #[:4000] if len(description_text) > 4000 else description_text
        
        client = get_openai_client()
        
        # Generate structured data in JSON format
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            response_format={ "type": "json_object" },
            messages=[
                {"role": "system", "content": (
                    "You are an expert at analyzing government contract opportunities. "
                    "Generate a clear, engaging title and structured summary based on the provided description. "
                    "Extract key details that would be most relevant to potential bidders and return them in a structured JSON format."
                )},
                {"role": "user", "content": (
                    f"Original Title: {opportunity_title}\n\n"
                    f"Description:\n{truncated_desc}\n\n"
                    "Please analyze this opportunity and return a JSON object with the following structure:\n"
                    "{\n"
                    "  \"title\": \"Improved, concise title (max 120 chars)\",\n"
                    "   " + SUMMARY_TEMPLATE + "\n}\n"
                    "If any information is not available in the description, use 'Not specified' as the value.\n"
                    "For contact information and due date, leave as empty string if not specified."
                )}
            ],
            temperature=0.2,
            max_tokens=max_length,
            n=1
        )
        
        # Parse the JSON response
        logger.info("OpenAI response received")
        content = response.choices[0].message.content.strip()
        # Extract JSON if wrapped in markdown
        if content.startswith("```json"):
            content = content.split("```json")[1].split("```", 1)[0].strip()
        result = json.loads(content)
        # Fallbacks
        title = result.get("title", opportunity_title or "Untitled Opportunity")
        summary = result.get("summary", DEFAULT_SUMMARY)
        # Normalize possible inline bullets into separate lines
        if isinstance(summary, dict):
            summary = normalize_bulleted_summary(summary)
        return {"title": title, "summary": summary}
    except Exception as e:
        logger.error(f"Title/Summary generation error: {str(e)}")
        return {
            "title": opportunity_title or "Untitled Opportunity",
            "summary": (
                "- Limited public details are available; please review the full notice for specifics.\n"
                "- Refer to the solicitation for financial terms, contract structure, and eligibility specifics.\n"
                "- Check the notice for submission deadlines and other key requirements."
            )
        }

# CSV_URL = "https://s3.amazonaws.com/falextracts/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv"
NOTICE_ID_COL = "NoticeId"
DESCRIPTION_COL = "Description"


def _parse_sortable_timestamp(value):
    """Parse incoming timestamp/date values for deterministic ordering."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    try:
        raw = str(value).strip()
        if not raw:
            return None
        # Normalize trailing Z for fromisoformat compatibility.
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except Exception:
        return None


def version_sort_key(item: Dict[str, Any]):
    """Canonical sort key for ordering versions newest-first."""
    return (
        _parse_sortable_timestamp(item.get("source_archive_date")) or datetime.min,
        _parse_sortable_timestamp(item.get("source_posted_at")) or datetime.min,
        _parse_sortable_timestamp(item.get("published_date")) or datetime.min,
        _parse_sortable_timestamp(item.get("ingested_at")) or datetime.min,
    )


def _is_low_information_description(text: str, min_chars: int = 80) -> bool:
    """Identify summary-hostile placeholder descriptions."""
    if not text:
        return True
    t = str(text).strip()
    if len(t) < min_chars:
        return True
    # Extremely repetitive one-liners should not dominate latest-first selection.
    tokens = t.lower().split()
    if len(set(tokens)) <= 3 and len(tokens) <= 12:
        return True
    return False


def fetch_versions_by_thread_key(thread_key: str, limit: int = 50) -> List[dict]:
    """
    Fetch versioned opportunity descriptions for a thread, newest first.

    This function is intentionally resilient; it returns [] if the table
    has not been migrated yet or if the query fails.
    """
    if not thread_key:
        return []
    try:
        limit = int(limit)
    except Exception:
        limit = 50
    if limit <= 0:
        limit = 50
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                notice_id,
                description,
                source_posted_at,
                source_archive_type,
                source_archive_date,
                ingested_at
            FROM ai_enhanced_opportunity_versions
            WHERE thread_key = %s
            ORDER BY
                source_archive_date DESC NULLS LAST,
                source_posted_at DESC NULLS LAST,
                ingested_at DESC
            LIMIT %s
            """,
            (thread_key, limit),
        )
        rows = cursor.fetchall()
        return [
            {
                "notice_id": row[0],
                "description": row[1] or "",
                "source_posted_at": row[2],
                "source_archive_type": row[3],
                "source_archive_date": row[4],
                "ingested_at": row[5],
            }
            for row in rows
        ]
    except Exception as e:
        logger.warning(f"Could not fetch version thread '{thread_key}': {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fetch_versions_by_thread_keys(thread_keys: List[str], per_thread_limit: int = 50) -> Dict[str, List[dict]]:
    """
    Fetch versioned descriptions for multiple threads in one query.
    Returns mapping: {thread_key: [version_rows_sorted_newest_first]}.
    """
    clean_keys = [str(k).strip() for k in (thread_keys or []) if str(k).strip()]
    if not clean_keys:
        return {}
    try:
        per_thread_limit = int(per_thread_limit)
    except Exception:
        per_thread_limit = 50
    if per_thread_limit <= 0:
        per_thread_limit = 50

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            WITH ranked AS (
                SELECT
                    thread_key,
                    notice_id,
                    description,
                    source_posted_at,
                    source_archive_type,
                    source_archive_date,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY thread_key
                        ORDER BY source_archive_date DESC NULLS LAST,
                                 source_posted_at DESC NULLS LAST,
                                 ingested_at DESC
                    ) AS rn
                FROM ai_enhanced_opportunity_versions
                WHERE thread_key = ANY(%s)
            )
            SELECT
                thread_key,
                notice_id,
                description,
                source_posted_at,
                source_archive_type,
                source_archive_date,
                ingested_at
            FROM ranked
            WHERE rn <= %s
            ORDER BY thread_key, rn
            """,
            (clean_keys, per_thread_limit),
        )
        rows = cursor.fetchall()
        out: Dict[str, List[dict]] = {}
        for row in rows:
            key = row[0]
            out.setdefault(key, []).append(
                {
                    "notice_id": row[1],
                    "description": row[2] or "",
                    "source_posted_at": row[3],
                    "source_archive_type": row[4],
                    "source_archive_date": row[5],
                    "ingested_at": row[6],
                }
            )
        return out
    except Exception as e:
        logger.warning(f"Could not batch fetch version threads: {e}")
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fetch_versions_by_thread_ids(thread_ids: List[str], per_thread_limit: int = 50) -> Dict[str, List[dict]]:
    """Fetch versions for multiple thread IDs, grouped and ordered newest-first."""
    clean_ids = [str(k).strip() for k in (thread_ids or []) if str(k).strip()]
    if not clean_ids:
        return {}
    try:
        per_thread_limit = int(per_thread_limit)
    except Exception:
        per_thread_limit = 50
    if per_thread_limit <= 0:
        per_thread_limit = 50

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            WITH ranked AS (
                SELECT
                    thread_id::text as thread_id,
                    notice_id,
                    description,
                    source_posted_at,
                    source_archive_type,
                    source_archive_date,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY thread_id
                        ORDER BY source_archive_date DESC NULLS LAST,
                                 source_posted_at DESC NULLS LAST,
                                 ingested_at DESC
                    ) AS rn
                FROM ai_enhanced_opportunity_versions
                WHERE thread_id::text = ANY(%s)
            )
            SELECT
                thread_id,
                notice_id,
                description,
                source_posted_at,
                source_archive_type,
                source_archive_date,
                ingested_at
            FROM ranked
            WHERE rn <= %s
            ORDER BY thread_id, rn
            """,
            (clean_ids, per_thread_limit),
        )
        rows = cursor.fetchall()
        out: Dict[str, List[dict]] = {}
        for row in rows:
            key = row[0]
            out.setdefault(key, []).append(
                {
                    "notice_id": row[1],
                    "description": row[2] or "",
                    "source_posted_at": row[3],
                    "source_archive_type": row[4],
                    "source_archive_date": row[5],
                    "ingested_at": row[6],
                }
            )
        return out
    except Exception as e:
        logger.warning(f"Could not batch fetch versions by thread ids: {e}")
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def build_prioritized_thread_description(
    current_opportunity: dict,
    persisted_versions: List[dict] = None,
    in_memory_versions: List[dict] = None,
    max_versions: int = 25,
    max_chars: int = 12000,
) -> str:
    """
    Build a summary input string that includes all known versions of a thread,
    ordered with latest entries first.
    """
    persisted_versions = persisted_versions or []
    in_memory_versions = in_memory_versions or []

    merged = []
    for item in in_memory_versions + persisted_versions:
        if not isinstance(item, dict):
            continue
        desc = (item.get("description") or "").strip()
        if not desc:
            continue
        merged.append(
            {
                "notice_id": item.get("notice_id"),
                "description": desc,
                "source_posted_at": item.get("source_posted_at"),
                "source_archive_type": item.get("source_archive_type"),
                "source_archive_date": item.get("source_archive_date"),
                "ingested_at": item.get("ingested_at"),
            }
        )

    # Ensure the current opportunity is included at least once.
    current_desc = (current_opportunity.get("description") or "").strip()
    if current_desc:
        merged.append(
            {
                "notice_id": current_opportunity.get("notice_id"),
                "description": current_desc,
                "source_posted_at": current_opportunity.get("source_posted_at"),
                "source_archive_type": current_opportunity.get("source_archive_type"),
                "source_archive_date": current_opportunity.get("source_archive_date"),
                "ingested_at": datetime.utcnow(),
            }
        )

    # Deduplicate same notice/version tuples while preserving newest records.
    deduped = {}
    for item in merged:
        key = (
            item.get("notice_id"),
            str(item.get("source_posted_at") or ""),
            str(item.get("source_archive_type") or ""),
            str(item.get("source_archive_date") or ""),
        )
        deduped[key] = item

    ordered = sorted(deduped.values(), key=version_sort_key, reverse=True)

    # If latest row is a low-information stub, promote the best richer row.
    if ordered and _is_low_information_description(ordered[0].get("description")):
        richer_idx = None
        for idx, candidate in enumerate(ordered[1:], start=1):
            if not _is_low_information_description(candidate.get("description")):
                richer_idx = idx
                break
        if richer_idx is not None:
            richer = ordered.pop(richer_idx)
            ordered.insert(0, richer)

    if max_versions and max_versions > 0:
        ordered = ordered[:max_versions]

    if not ordered:
        return current_desc

    blocks = []
    char_count = 0
    for idx, item in enumerate(ordered, start=1):
        notice = item.get("notice_id") or "unknown"
        posted = item.get("source_posted_at") or ""
        archive_type = item.get("source_archive_type") or ""
        archive_date = item.get("source_archive_date") or ""
        block = (
            f"[Version {idx}] notice_id={notice} posted={posted} archive_type={archive_type} archive_date={archive_date}\n"
            f"{item.get('description')}"
        )
        if max_chars > 0 and char_count + len(block) > max_chars:
            break
        blocks.append(block)
        char_count += len(block) + 2
    return "\n\n".join(blocks)

def find_descriptions_by_notice_ids(notice_ids: List[str], chunksize: int = 5000) -> List[dict]:
    # if not notice_ids:
    #     return []

    # notice_id_set = set(notice_ids)
    # found_map = {}

    # try:
    #     csv_path = Path(__file__).resolve().parents[2] / "public" / "ContractOpportunitiesFullCSV.csv"

    #     # for chunk in pd.read_csv(CSV_URL, chunksize=chunksize, usecols=[NOTICE_ID_COL, DESCRIPTION_COL], dtype=str, encoding="cp1252"):
    #     for chunk in pd.read_csv(csv_path, chunksize=chunksize, usecols=[NOTICE_ID_COL, DESCRIPTION_COL], dtype=str, encoding="cp1252"):

    #         chunk_filtered = chunk[chunk[NOTICE_ID_COL].isin(notice_id_set)]
    #         for _, row in chunk_filtered.iterrows():
    #             nid = row[NOTICE_ID_COL]
    #             if nid not in found_map:
    #                 found_map[nid] = row[DESCRIPTION_COL]
    #             if len(found_map) == len(notice_id_set):
    #                 break
    #     return [{NOTICE_ID_COL: nid, DESCRIPTION_COL: desc} for nid, desc in found_map.items()]
    # except Exception as e:
    #     logger.error(f"Error processing CSV: {str(e)}")
    #     return []

    """
    Retrieve descriptions for given notice IDs from the sam_gov_csv table.

    Args:
        notice_ids: List of notice IDs to search for.

    Returns:
        List of dictionaries containing notice IDs and their descriptions.
        Format: [{"NoticeId": str, "Description": str}, ...]
    """
    if not notice_ids:
        logger.info("No notice IDs provided, returning empty list")
        return []

    conn = None
    cursor = None
    try:
        # Connect to database        
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query the sam_gov_csv table for notice_ids
        query = "SELECT notice_id, description FROM sam_gov_csv WHERE notice_id IN %s"
        # execute_values(cursor, query, [(nid,) for nid in notice_ids], fetch=True)
        # results = cursor.fetchall()
        cursor.execute(query, (tuple(notice_ids),))  # pass a single tuple as parameter
        results = cursor.fetchall()

        # Map results to the required format
        found_map = {row[0]: row[1] for row in results}
        result = [
            {NOTICE_ID_COL: nid, DESCRIPTION_COL: found_map.get(nid, "")}
            for nid in notice_ids
        ]

        logger.info(f"Retrieved {len(found_map)} descriptions for {len(notice_ids)} notice IDs")
        return result

    except psycopg2.Error as e:
        logger.error(f"Database error while fetching descriptions: {str(e)}")
        return [{NOTICE_ID_COL: nid, DESCRIPTION_COL: ""} for nid in notice_ids]
    except Exception as e:
        logger.error(f"Unexpected error while fetching descriptions: {str(e)}")
        return [{NOTICE_ID_COL: nid, DESCRIPTION_COL: ""} for nid in notice_ids]
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def process_opportunity_descriptions(opportunity: dict):
    """
    Processes a list of opportunities to generate summaries for their descriptions.
    """
    try:
        logger.info(f"Processing opportunity for summary generation")

        # Attach additional descriptions
        nid = opportunity.get("notice_id")
        if nid and not opportunity.get("additional_description"):
            opportunity["additional_description"] = find_descriptions_by_notice_ids([nid])[0][DESCRIPTION_COL] 
        opportunity = await process_opportunity_summaries(opportunity)
        return opportunity

    except Exception as e:
        logger.error(f"Error processing opportunity description: {str(e)}")
        return opportunity
    
DEFAULT_SUMMARY = {
    "sponsor": "Not specified\n",
    "objective": "Not specified\n",
    "goal": "Not specified\n",
    "eligibility": "Not specified\n",
    "key_facts": "Not specified\n",
    "contact_info": "\n",
    "due_date": "\n"
}

async def process_opportunity_summaries(opportunity):
    """
    Processes a list of opportunities to generate summaries and improved titles for their descriptions.
    """
    try:
        logger.info(f"Processing opportunity for summary and title generation")

        async def summarize_opportunity(opp):
            description = opp.get("additional_description")
            orig_title = opp.get("title") or opp.get("opportunity_title") or ""
            if description:
                try:
                    result = await generate_title_and_summary(orig_title, description)
                    opp["title"] = result["title"]
                    opp["summary"] = result["summary"]
                except Exception as e:
                    logger.warning(f"Failed to generate title/summary for Notice ID {opp.get('noticeid')}: {e}")
                    opp["title"] = orig_title or "Untitled Opportunity"
                    opp["summary"] = DEFAULT_SUMMARY
            else:
                opp["title"] = orig_title or "Untitled Opportunity"
                opp["summary"] = DEFAULT_SUMMARY

        # async for opp in asyncio.as_completed((summarize_opportunity(opp) for opp in opportunities)):
        #     yield opp
        await summarize_opportunity(opportunity)
        return opportunity

    except Exception as e:
        logger.error(f"Error processing opportunity summaries: {str(e)}")
        return opportunity
