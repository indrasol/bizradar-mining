import time
import asyncio
from typing import List, Dict

# Pronouns and function words to filter during analysis
FILTER_WORDS = set([
    'they', 'their', 'them', 'he', 'she', 'his', 'her', 'it', 'its', 'we', 'our', 'us', 'you', 'your',
    'the', 'and', 'that', 'this', 'with', 'for', 'from', 'was', 'were', 'been', 'has', 'have', 'had',
    'can', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'over', 'are', 'who', 'what',
    'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'more', 'most', 'some', 'such',
    'about', 'above', 'across', 'after', 'against', 'along', 'among', 'around', 'before', 'behind',
    'below', 'beneath', 'beside', 'between', 'beyond', 'during', 'except', 'inside', 'into', 'like',
    'near', 'off', 'onto', 'outside', 'since', 'through', 'throughout', 'under', 'until', 'upon',
    'within', 'without'
])

def generate_enhanced_markdown(pages_data: List[Dict], base_domain: str) -> str:
    md_content = f"# Web Content Analysis: {base_domain}\n\n"
    md_content += f"Date: {time.strftime('%Y-%m-%d')}\n\n"
    md_content += "## Executive Summary\n\n"

    total_words = sum(p.get('content_analysis', {}).get('word_count', 0) for p in pages_data)
    md_content += f"This analysis covers {len(pages_data)} pages from {base_domain} with approximately {total_words} words.\n\n"

    # Topic Analysis
    topic_freq = {}
    for p in pages_data:
        topics = p.get('content_analysis', {}).get('topics', [])
        for topic in topics:
            topic_lower = topic.lower()
            topic_freq[topic_lower] = topic_freq.get(topic_lower, 0) + 1
    top_topics = sorted(topic_freq.items(), key=lambda x: x[1], reverse=True)[:5]
    if top_topics:
        md_content += "Main topics include: " + ", ".join([t[0] for t in top_topics]) + ".\n\n"

    md_content += "## Table of Contents\n\n"
    unique_pages = {p['title']: p for p in pages_data}
    for i, title in enumerate(unique_pages.keys(), 1):
        md_content += f"{i}. [{title}](#{title.lower().replace(' ', '-').replace(':', '')})\n"
    md_content += "\n"

    for title, p in unique_pages.items():
        anchor = title.lower().replace(' ', '-').replace(':', '')
        md_content += f"## {title}\n\n**URL**: {p['url']}\n\n"

        ca = p.get('content_analysis', {})
        if ca.get('summary'):
            md_content += f"**Summary**: {ca['summary']}\n\n"

        if ca.get('topics'):
            md_content += "### Topics\n" + ''.join(f"- {t}\n" for t in ca['topics']) + "\n"

        if ca.get('key_points'):
            md_content += "### Key Points\n" + ''.join(f"- {kp}\n" for kp in ca['key_points']) + "\n"

        if ca.get('top_words'):
            filtered = [w for w in ca['top_words'] if w.lower() not in FILTER_WORDS][:20]
            word_groups = [filtered[i:i+5] for i in range(0, len(filtered), 5)]
            md_content += "### Frequent Terms\n" + ''.join(f"- {', '.join(g)}\n" for g in word_groups) + "\n"

        if ca.get('word_count'):
            md_content += f"**Content Size**: {ca['word_count']} words\n\n"

        images = p.get('images', [])
        if images:
            md_content += "### Visual Content\n"
            with_text = [img for img in images if img.get('extracted_text') and "No text" not in img['extracted_text']]
            without_text = [img for img in images if not img.get('extracted_text')]

            if with_text:
                md_content += "#### Text Extracted from Images\n"
                for i, img in enumerate(with_text, 1):
                    alt = img.get('alt', 'No alt text')
                    md_content += f"**Image {i}**: {alt} ({img.get('width')}x{img.get('height')})\n\n"
                    md_content += f"```\n{img['extracted_text']}\n```\n\n"

            if without_text:
                md_content += f"**Additional Visual Elements**: {len(without_text)} without extractable text\n\n"

        md_content += "---\n\n"

    # Content Insights Section
    md_content += "## Content Insights\n\n"

    # Page word count
    word_counts = sorted(
        [(p['title'], p.get('content_analysis', {}).get('word_count', 0)) for p in pages_data],
        key=lambda x: x[1], reverse=True
    )
    md_content += "### Content Distribution\n\n"
    for title, wc in word_counts:
        md_content += f"- {title}: {wc} words\n"
    md_content += "\n"

    # Site-wide top words
    all_words = []
    for p in pages_data:
        all_words += p.get('content_analysis', {}).get('top_words', [])
    word_freq = {}
    for w in all_words:
        wl = w.lower()
        if wl not in FILTER_WORDS and len(wl) > 2:
            word_freq[wl] = word_freq.get(wl, 0) + 1
    common = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:25]
    if common:
        md_content += "### Key Terminology\n\n"
        groups = [common[i:i+5] for i in range(0, len(common), 5)]
        for group in groups:
            terms = [f"{w} ({c})" for w, c in group]
            md_content += f"- {', '.join(terms)}\n"

    return md_content


async def generate_markdown_from_scraper(scraper_instance, output_path=None) -> str:
    pages = await scraper_instance.crawl()
    domain = scraper_instance.base_domain
    markdown = generate_enhanced_markdown(pages, domain)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown)
        print(f"[✔] Markdown saved to {output_path}")

    return markdown


# ✅ Wrapper for importing from other files
def generate_company_markdown(scraper_instance, output_path=None) -> str:
    return asyncio.run(generate_markdown_from_scraper(scraper_instance, output_path))
