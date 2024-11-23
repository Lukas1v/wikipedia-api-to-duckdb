{{ config(materialized='table') }}
SELECT 
    -- Handling BOOLEAN conversions for actionhidden, anon, bot, commenthidden, minor, new, redirect, sha1hidden, suppressed, userhidden
    CASE 
        WHEN actionhidden = '' THEN TRUE
        WHEN actionhidden IS NULL THEN FALSE
        ELSE actionhidden::BOOLEAN
    END AS actionhidden,
    CASE 
        WHEN anon = '' THEN TRUE
        WHEN anon IS NULL THEN FALSE
        ELSE anon::BOOLEAN
    END AS anon,
    CASE 
        WHEN bot = '' THEN TRUE
        WHEN bot IS NULL THEN FALSE
        ELSE bot::BOOLEAN
    END AS bot,
    comment, 
    CASE 
        WHEN commenthidden = '' THEN TRUE
        WHEN commenthidden IS NULL THEN FALSE
        ELSE commenthidden::BOOLEAN
    END AS commenthidden,
    logaction,
    CAST(logid AS INTEGER) AS logid,
    logparams,
    logtype,
    CASE 
        WHEN minor = '' THEN TRUE
        WHEN minor IS NULL THEN FALSE
        ELSE minor::BOOLEAN
    END AS minor,
    CASE 
        WHEN new = '' THEN TRUE
        WHEN new IS NULL THEN FALSE
        ELSE new::BOOLEAN
    END AS new,
    CAST(newlen AS INTEGER) AS newlen,
    CAST(ns AS INTEGER) AS ns,
    CAST(old_revid AS INTEGER) AS old_revid,
    CAST(oldlen AS INTEGER) AS oldlen,
    CAST(pageid AS INTEGER) AS pageid,
    parsedcomment,
    CAST(rcid AS INTEGER) AS rcid,
    CASE 
        WHEN redirect = '' THEN TRUE
        WHEN redirect IS NULL THEN FALSE
        ELSE redirect::BOOLEAN
    END AS redirect,
    CAST(revid AS INTEGER) AS revid,
    sha1,
    CASE 
        WHEN sha1hidden = '' THEN TRUE
        WHEN sha1hidden IS NULL THEN FALSE
        ELSE sha1hidden::BOOLEAN
    END AS sha1hidden,
    CASE 
        WHEN suppressed = '' THEN TRUE
        WHEN suppressed IS NULL THEN FALSE
        ELSE suppressed::BOOLEAN
    END AS suppressed,
    tags,
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    title,
    type,
    user,
    CASE 
        WHEN userhidden = '' THEN TRUE
        WHEN userhidden IS NULL THEN FALSE
        ELSE userhidden::BOOLEAN
    END AS userhidden,
    CAST(userid AS INTEGER) AS userid
FROM {{ source('main', 'bronze_recent_changes') }}