-- ============================================================================
-- Maverick AI Tutor — Flink SQL job
-- ============================================================================
--
-- ARCHITECTURE NOTE (post-Schema Registry):
--   Both topics now have JSON Schemas registered in Confluent Schema Registry
--   under the standard TopicNameStrategy subjects (`<topic>-value`). This file
--   uses `'value.format' = 'json-registry'` so Flink reads/writes the Confluent
--   wire format (magic byte + schema id + payload). The Node producer in
--   src/kafka.js does the same via @kafkajs/confluent-schema-registry — so
--   Flink and Node interoperate on the wire.
--
-- Paste this whole file into:
--   Confluent Cloud → Flink → SQL Workspace → New statement
--
-- (a) WHICH ENV + CLUSTER
--     Before running, in the SQL Workspace top bar, select:
--       Environment : <your env>            (e.g. 'maverick-dev')
--       Catalog     : same as the environment name
--       Database    : the Kafka cluster name (e.g. 'maverick-cluster')
--     The Kafka topics `learning.events` and `learner.recommendations` must
--     already exist in that cluster — Track B notes covers creating them.
--
-- (b) PREREQ — SCHEMAS REGISTERED
--     Run `npm run register-schemas` (locally) once after CONFLUENT_SR_* creds
--     are in .env. That registers /schemas/learning.events-value.json and
--     /schemas/learner.recommendations-value.json under the subjects:
--       learning.events-value
--       learner.recommendations-value
--     Flink will find them automatically via `json-registry`.
--
-- (c) HOW TO REGISTER `next_lesson_model` (only needed for the primary path)
--     Confluent Cloud → AI Model Inference → New
--       Provider : Anthropic
--       Model    : claude-3-5-sonnet-20241022 (or current Claude in CC)
--       Task     : TEXT_GENERATION
--       Name     : next_lesson_model
--       Endpoint : (Anthropic default)
--       API Key  : your Anthropic key
--     Save. The model now appears as a callable function inside Flink SQL.
--
-- (d) PLAN B — if ML_PREDICT is NOT enabled in your CC tier
--     Comment OUT the "PRIMARY PATH" INSERT statement and comment IN the
--     "FALLBACK PATH" INSERT statement below. The fallback skips Claude and
--     copies `performance.growth_areas` straight into next_scenario_prompt.
--     `src/recommender.js` (deliverer-only mode) still produces a working
--     adaptive follow-up.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Source: learning events  (produced by server.js /webhook/waterr)
-- ----------------------------------------------------------------------------
CREATE TABLE learning_events (
  schema_version INT,
  event_id       STRING,
  occurred_at    TIMESTAMP_LTZ(3),
  learner        ROW<id STRING, display_name STRING, contact_channel STRING>,
  meeting        ROW<id STRING, scenario_id STRING, subject STRING, duration_seconds INT>,
  performance    ROW<
    average_score INT,
    goal_results  ARRAY<ROW<goal STRING, score INT, feedback STRING>>,
    filler_word_rate DOUBLE,
    growth_areas  STRING,
    strengths     STRING
  >,
  raw_transcript_ref STRING,
  WATERMARK FOR occurred_at AS occurred_at - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'confluent',
  'topic'        = 'learning.events',
  'value.format' = 'json-registry'
);


-- ----------------------------------------------------------------------------
-- Sink: lesson recommendations  (consumed by src/recommender.js)
-- ----------------------------------------------------------------------------
CREATE TABLE learner_recommendations (
  schema_version INT,
  event_id       STRING,
  occurred_at    TIMESTAMP_LTZ(3),
  learner_id     STRING,
  contact_channel STRING,
  recommendation ROW<
    subject STRING,
    difficulty STRING,
    next_scenario_prompt STRING,
    estimated_duration_min INT
  >
) WITH (
  'connector'    = 'confluent',
  'topic'        = 'learner.recommendations',
  'value.format' = 'json-registry'
);


-- ============================================================================
-- PRIMARY PATH — uses ML_PREDICT against the registered `next_lesson_model`.
-- (This is the demo's "AI inside the stream" moment.)
-- ============================================================================
INSERT INTO learner_recommendations
SELECT
  1                                              AS schema_version,
  CONCAT('rec_', event_id)                       AS event_id,
  CURRENT_TIMESTAMP                              AS occurred_at,
  learner.id                                     AS learner_id,
  learner.contact_channel                        AS contact_channel,
  ROW(
    meeting.subject,
    CASE
      WHEN performance.average_score < 60 THEN 'easier'
      WHEN performance.average_score > 85 THEN 'harder'
      ELSE 'same'
    END,
    -- Single ML_PREDICT call generates the full next-lesson prompt
    ML_PREDICT(
      'next_lesson_model',
      CONCAT(
        'You are designing the next 12-minute lesson for a blind learner. ',
        'Subject: ', meeting.subject, '. ',
        'Previous score: ', CAST(performance.average_score AS STRING), '/100. ',
        'Growth areas: ', performance.growth_areas, '. ',
        'Output ONLY the new tutor system prompt — strict audio-only language, ',
        'sound-based analogies, ask learner to explain back. No preamble.'
      )
    ),
    12
  )                                              AS recommendation
FROM learning_events
WHERE performance.average_score IS NOT NULL;


-- ============================================================================
-- FALLBACK PATH — if ML_PREDICT is NOT available (no AI Model Inference
-- on your CC tier, or `next_lesson_model` isn't registered yet).
--
-- HOW TO ACTIVATE:
--   1. Comment out the PRIMARY PATH `INSERT` above (wrap in /* ... */).
--   2. Remove the leading `/*` and trailing `*/` from this block.
--
-- This copies `growth_areas` straight into next_scenario_prompt with a small
-- audio-only-language preamble. `src/recommender.js` will still create a real
-- Waterr scenario from it (scenarios/create-with-gpt expands a short prompt
-- into a full scenario anyway).
-- ============================================================================
/*
INSERT INTO learner_recommendations
SELECT
  1                                              AS schema_version,
  CONCAT('rec_', event_id)                       AS event_id,
  CURRENT_TIMESTAMP                              AS occurred_at,
  learner.id                                     AS learner_id,
  learner.contact_channel                        AS contact_channel,
  ROW(
    meeting.subject,
    CASE
      WHEN performance.average_score < 60 THEN 'easier'
      WHEN performance.average_score > 85 THEN 'harder'
      ELSE 'same'
    END,
    CONCAT(
      'You are Aarya, a patient blind-friendly tutor. ',
      'Subject: ', meeting.subject, '. ',
      'Previous score: ', CAST(performance.average_score AS STRING), '/100. ',
      'Re-teach this with strict audio-only language and sound-based analogies. ',
      'Specific growth areas from the last session: ',
      COALESCE(performance.growth_areas, 'review the basics one more time'), '. ',
      'End by asking the learner to explain it back in their own words.'
    ),
    12
  )                                              AS recommendation
FROM learning_events
WHERE performance.average_score IS NOT NULL;
*/
