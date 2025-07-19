def is_homepage(trimmed_page_url: str, site: str) -> bool:
    if not trimmed_page_url or not site:
        return False
    
    trimmed = trimmed_page_url.strip().lower().rstrip("/")
    site_clean = site.strip().lower().replace("www.", "")

    return trimmed == site_clean