from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col, when, lower, trim, lit, concat, sha2, split, regexp_replace, rlike,
    regexp_extract, coalesce, length
)

def transform_data(df: DataFrame, capture_rejects: bool = False):
    """
    Clean, normalize, and drop non-public/internal URLs as early as possible.
    Returns either a DataFrame (valid rows) or (valid_df, rejects_df) if capture_rejects=True.
    """
    print("[TRANSFORM] Starting URL cleanup and deduplication")

    invalids = ["(not set)", "none", "null", ""]
    # Normalize the string fields we depend on
    d0 = df.withColumn("trimmed_page_url", trim(lower(col("trimmed_page_url")))) \
           .withColumn("site", trim(lower(col("site"))))

    d1 = d0.filter(~col("trimmed_page_url").isin(invalids) & ~col("site").isin(invalids))

    # --- Derive host candidates (no scheme), then choose best host ---
    # Remove scheme from url/site candidates and take the first token before '/'
    url_no_scheme  = regexp_replace(coalesce(col("trimmed_page_url"), lit("")), r"^https?://", "")
    site_no_scheme = regexp_replace(coalesce(col("site"),            lit("")), r"^https?://", "")

    url_host_guess  = split(url_no_scheme,  "/").getItem(0)
    site_host_guess = split(site_no_scheme, "/").getItem(0)

    # netloc keeps potential :port for a simple port check
    netloc = when(url_host_guess.rlike(r"[.:]"), url_host_guess).otherwise(site_host_guess)

    # choose host (strip leading 'www.')
    host = regexp_replace(
        when(url_host_guess.rlike(r"[.:]"), url_host_guess).otherwise(site_host_guess),
        r"^www\.", ""
    )

    # Derive useful flags
    tld = regexp_extract(host, r"\.([^.]+)$", 1)

    is_single_label    = ~host.rlike(r"\.")                         # no dot
    is_ipv4_literal    = host.rlike(r"^\d{1,3}(\.\d{1,3}){3}$")
    has_port           = netloc.rlike(r":\d+$")
    has_underscore     = host.rlike(r"_")
    ends_with_local    = host.rlike(r"\.local(\.|$)")
    is_reserved_tld    = tld.isin("test","local","localhost","invalid","example")

    # Your internal/vendor patterns
    is_internal_suffix = host.rlike(r"\.local\.simpleviewcms\.com$") | host.rlike(r"\.cms30\.localhost$")
    is_internal_regex  = host.rlike(r".*\.staging\.simpleviewcms\.com$") | host.rlike(r"(^|\.)primary[\d-]*-.*\.simpleviewcms\.com$")

    # Compose drop reason (first matching wins, ordered by "most certain")
    drop_reason = when(is_reserved_tld | ends_with_local,            lit("reserved_or_local")) \
        .when(is_internal_suffix,                                    lit("internal_suffix")) \
        .when(is_internal_regex,                                     lit("internal_pattern")) \
        .when(is_single_label,                                       lit("single_label_host")) \
        .when(is_ipv4_literal,                                       lit("ipv4_literal")) \
        .when(has_port,                                              lit("explicit_port")) \
        .when(has_underscore,                                        lit("underscore_in_host")) \
        .otherwise(lit(None))

    d2 = d1.withColumn("host", host) \
           .withColumn("netloc", netloc) \
           .withColumn("tld", tld) \
           .withColumn("drop_reason", drop_reason)

    # Keep only public / resolvable-looking rows
    valid = d2.filter(col("drop_reason").isNull())
    rejects = d2.filter(col("drop_reason").isNotNull()) \
                .select("site","trimmed_page_url","host","drop_reason")

    # --- Build page_url only for valid rows (avoids nonsense like 'https://us minor outlying islands') ---
    v = valid.withColumn(
        "page_url",
        when(col("trimmed_page_url").startswith("http"), col("trimmed_page_url"))
        .when(col("trimmed_page_url").startswith("//"), concat(lit("https:"), col("trimmed_page_url")))
        .when(col("trimmed_page_url").startswith("/"),
              concat(lit("https://"), col("host"), col("trimmed_page_url")))
        # If it's not a path and got this far, it should look like a host[/path]
        .otherwise(concat(lit("https://"), col("trimmed_page_url")))
    )

    # Only compute url_hash if extractor didn’t provide it (keeps Option A zero-copy)
    if "url_hash" not in v.columns:
        v = v.withColumn("url_hash", sha2(col("trimmed_page_url").cast(StringType()), 256))

    total = v.count()
    print(f"[TRANSFORM] Valid/public URLs: {total} rows")

    if capture_rejects:
        return v, rejects
    return v
