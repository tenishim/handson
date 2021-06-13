create or replace table ga.fa_users_201809 as
select
  user_pseudo_id
, max(session_start) = 1 as single
, count(1) as days
, round(sum(ad_reward) / sum(session_start), 1) as ad_reward
, round(sum(app_clear_data) / sum(session_start), 1) as app_clear_data
, round(sum(app_exception) / sum(session_start), 1) as app_exception
, round(sum(app_remove) / sum(session_start), 1) as app_remove
, round(sum(app_update) / sum(session_start), 1) as app_update
, round(sum(challenge_a_friend) / sum(session_start), 1) as challenge_a_friend
, round(sum(challenge_accepted) / sum(session_start), 1) as challenge_accepted
, round(sum(completed_5_levels) / sum(session_start), 1) as completed_5_levels
, round(sum(dynamic_link_app_open) / sum(session_start), 1) as dynamic_link_app_open
, round(sum(dynamic_link_first_open) / sum(session_start), 1) as dynamic_link_first_open
, round(sum(error) / sum(session_start), 1) as error
, round(sum(firebase_campaign) / sum(session_start), 1) as firebase_campaign
, sum(first_open) > 0 as first_open
, round(sum(in_app_purchase) / sum(session_start), 1) as in_app_purchase
, round(sum(level_complete) / sum(session_start), 1) as level_complete
, round(sum(level_complete_quickplay) / sum(session_start), 1) as level_complete_quickplay
, round(sum(level_end) / sum(session_start), 1) as level_end
, round(sum(level_end_quickplay) / sum(session_start), 1) as level_end_quickplay
, round(sum(level_fail) / sum(session_start), 1) as level_fail
, round(sum(level_fail_quickplay) / sum(session_start), 1) as level_fail_quickplay
, round(sum(level_reset) / sum(session_start), 1) as level_reset
, round(sum(level_reset_quickplay) / sum(session_start), 1) as level_reset_quickplay
, round(sum(level_retry) / sum(session_start), 1) as level_retry
, round(sum(level_retry_quickplay) / sum(session_start), 1) as level_retry_quickplay
, round(sum(level_start) / sum(session_start), 1) as level_start
, round(sum(level_start_quickplay) / sum(session_start), 1) as level_start_quickplay
, round(sum(level_up) / sum(session_start), 1) as level_up
, round(sum(no_more_extra_steps) / sum(session_start), 1) as no_more_extra_steps
, round(sum(os_update) / sum(session_start), 1) as os_update
, round(sum(post_score) / sum(session_start), 1) as post_score
, round(sum(screen_view) / sum(session_start), 1) as screen_view
, round(sum(select_content) / sum(session_start), 1) as select_content
, round(sum(spend_virtual_currency) / sum(session_start), 1) as spend_virtual_currency
, round(sum(use_extra_steps) / sum(session_start), 1) as use_extra_steps
, round(sum(user_engagement) / sum(session_start), 1) as user_engagement
from
  (
    select
      user_pseudo_id
    , event_date
    , countif(event_name = 'ad_reward') as ad_reward
    , countif(event_name = 'app_clear_data') as app_clear_data
    , countif(event_name = 'app_exception') as app_exception
    , countif(event_name = 'app_remove') as app_remove
    , countif(event_name = 'app_update') as app_update
    , countif(event_name = 'challenge_a_friend') as challenge_a_friend
    , countif(event_name = 'challenge_accepted') as challenge_accepted
    , countif(event_name = 'completed_5_levels') as completed_5_levels
    , countif(event_name = 'dynamic_link_app_open') as dynamic_link_app_open
    , countif(event_name = 'dynamic_link_first_open') as dynamic_link_first_open
    , countif(event_name = 'error') as error
    , countif(event_name = 'firebase_campaign') as firebase_campaign
    , countif(event_name = 'first_open') as first_open
    , countif(event_name = 'in_app_purchase') as in_app_purchase
    , countif(event_name = 'level_complete') as level_complete
    , countif(event_name = 'level_complete_quickplay') as level_complete_quickplay
    , countif(event_name = 'level_end') as level_end
    , countif(event_name = 'level_end_quickplay') as level_end_quickplay
    , countif(event_name = 'level_fail') as level_fail
    , countif(event_name = 'level_fail_quickplay') as level_fail_quickplay
    , countif(event_name = 'level_reset') as level_reset
    , countif(event_name = 'level_reset_quickplay') as level_reset_quickplay
    , countif(event_name = 'level_retry') as level_retry
    , countif(event_name = 'level_retry_quickplay') as level_retry_quickplay
    , countif(event_name = 'level_start') as level_start
    , countif(event_name = 'level_start_quickplay') as level_start_quickplay
    , countif(event_name = 'level_up') as level_up
    , countif(event_name = 'no_more_extra_steps') as no_more_extra_steps
    , countif(event_name = 'os_update') as os_update
    , countif(event_name = 'post_score') as post_score
    , countif(event_name = 'screen_view') as screen_view
    , countif(event_name = 'select_content') as select_content
    , countif(event_name = 'session_start') as session_start
    , countif(event_name = 'spend_virtual_currency') as spend_virtual_currency
    , countif(event_name = 'use_extra_steps') as use_extra_steps
    , countif(event_name = 'user_engagement') as user_engagement
    from
      `firebase-public-project.analytics_153293282.events_*`
    where
      _table_suffix between '20180901' and '20180930'
    group by
      user_pseudo_id
    , event_date
    having
      session_start > 0
  )
group by
  user_pseudo_id
having
  days > 1
and (max(session_start) = 1 or min(session_start) > 1)
