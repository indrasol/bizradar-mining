"""
Constants for cron jobs
"""
import os
from datetime import datetime, timedelta

# URLs
FREELANCER_URL = "https://www.freelancer.com/search/projects?funnel=true&projectLanguages=en&projectSkills=305"
SAM_GOV_API_URL = "https://api.sam.gov/prod/opportunities/v2/search"

# Headers
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Table names
FREELANCER_TABLE = "freelancer_projects"
SAM_GOV_TABLE = "sam_gov"

# SAM.gov API configuration
from sam_gov.config.settings import SAM_API_KEY as api_key
SAM_GOV_PARAMS = {
    "api_key": api_key,
    "q": "cybersecurity",
    "ncode": "541519",
    "postedFrom": (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y"),
    "postedTo": datetime.now().strftime("%m/%d/%Y"),
    "limit": 1000,
    "offset": 0
}

# Freelancer selectors
FREELANCER_SELECTORS = {
    "project_item": "div.JobSearchCard-item",
    "title": ["a", "h3", "span"],
    "title_class": lambda x: x and "JobSearchCard-primary-heading" in x,
    "price": "div.JobSearchCard-primary-price",
    "bids": "div.JobSearchCard-secondary-entry",
    "skills": "a.JobSearchCard-primary-tags",
    "details": "div.JobSearchCard-secondary"
}