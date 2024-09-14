{{ config(
    materialized='view'
) }}

SELECT 
    CAST(date AS DATE) AS date,
    CAST(NULL AS INT64) AS add_to_cart,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(channel AS STRING) AS channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id,
    CAST(clicks AS INT64) AS clicks,
    CAST(NULL AS INT64) AS comments, 
    CAST(NULL AS INT64) AS engagements,
    CAST(imps AS INT64) AS impressions,
    CAST(NULL AS INT64) AS installs,
    CAST(NULL AS INT64) AS likes,
    CAST(NULL AS INT64) AS link_clicks,
    CAST(NULL AS INT64) AS post_click_conversions,
    CAST(NULL AS INT64) AS post_view_conversions,
    CAST(NULL AS INT64) AS posts,
    CAST(NULL AS INT64) AS purchase,
    CAST(NULL AS INT64) AS registrations,
    CAST(revenue AS INT64) AS revenue,
    CAST(NULL AS INT64) AS shares,
    CAST(spend AS INT64) AS spend,
    CAST(conv AS INT64) AS total_conversions,
    CAST(NULL AS INT64) AS video_views
FROM {{ ref('src_ads_bing_all_data')}}

UNION ALL 

SELECT 
    CAST(date AS DATE) AS date,
    CAST(add_to_cart AS INT64) AS add_to_cart,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(channel AS STRING) AS channel,
    CAST(creative_id AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id,
    CAST(clicks AS INT64) AS clicks, 
    CAST(comments AS INT64) AS comments, 
    CAST((views+clicks+comments+likes+purchase+complete_registration+shares) AS INT64) AS engagements,
    CAST(impressions AS INT64) AS impressions,
    CAST(mobile_app_install AS INT64) AS installs,
    CAST(likes AS INT64) AS likes,
    CAST(inline_link_clicks AS INT64) AS link_clicks,
    CAST(NULL AS INT64) AS post_click_conversions,
    CAST(NULL AS INT64) AS post_view_conversions,
    CAST(NULL AS INT64) AS posts,
    CAST(purchase_value AS INT64) AS purchase,
    CAST(complete_registration AS INT64) AS registrations,
    CAST(NULL AS INT64) AS revenue,
    CAST(shares AS INT64) AS shares,
    CAST(spend AS INT64) AS spend,
    CAST(purchase AS INT64) AS total_conversions,
    CAST(views AS INT64) AS video_views
FROM {{ ref('src_ads_creative_facebook_all_data')}}

UNION ALL

SELECT 
    CAST(date AS DATE) AS date,
    CAST(add_to_cart AS INT64) AS add_to_cart,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(NULL AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(channel AS STRING) AS channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id,
    CAST(clicks AS INT64) AS clicks,
    CAST(NULL AS INT64) AS comments, 
    CAST(NULL AS INT64) AS engagements,
    CAST(impressions AS INT64) AS impressions,
    CAST((rt_installs + skan_app_install) AS INT64) AS installs,
    CAST(NULL AS INT64) AS likes,
    CAST(NULL AS INT64) AS link_clicks,
    CAST(NULL AS INT64) AS post_click_conversions,
    CAST(NULL AS INT64) AS post_view_conversions,
    CAST(NULL AS INT64) AS posts,
    CAST(purchase AS INT64) AS purchase,
    CAST(registrations AS INT64) AS registrations,
    CAST(NULL AS INT64) AS revenue,
    CAST(NULL AS INT64) AS shares,
    CAST(spend AS INT64) AS spend,
    CAST((conversions + skan_conversion) AS INT64) AS total_conversions,
    CAST(video_views AS INT64) AS video_views
FROM {{ ref('src_ads_tiktok_ads_all_data')}}

UNION ALL

SELECT 
    CAST(date AS DATE) AS date,
    CAST(NULL AS INT64) AS add_to_cart,
    CAST(NULL AS STRING) AS ad_id,
    CAST(NULL AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(channel AS STRING) AS channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id,
    CAST(clicks AS INT64) AS clicks,
    CAST(comments AS INT64) AS comments, 
    CAST(engagements AS INT64) AS engagements,
    CAST(impressions AS INT64) AS impressions,
    CAST(NULL AS INT64) AS installs,
    CAST(likes AS INT64) AS likes,
    CAST(url_clicks AS INT64) AS link_clicks,
    CAST(NULL AS INT64) AS post_click_conversions,
    CAST(NULL AS INT64) AS post_view_conversions,
    CAST(NULL AS INT64) AS posts, 
    CAST(NULL AS INT64) AS purchase,
    CAST(NULL AS INT64) AS registrations,
    CAST(NULL AS INT64) AS revenue,
    CAST(retweets AS INT64) AS shares,
    CAST(spend AS INT64) AS spend,
    CAST(NULL AS INT64) AS total_conversions,
    CAST(video_total_views AS INT64) AS video_views
FROM {{ ref('src_promoted_tweets_twitter_all_data')}}
