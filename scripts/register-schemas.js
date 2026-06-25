// scripts/register-schemas.js
//
// One-shot CLI: registers /schemas/*-value.json against Confluent Cloud
// Schema Registry under the conventional `<topic>-value` subject names.
//
// Run after Harshit fills CONFLUENT_SCHEMA_REGISTRY_URL +
// CONFLUENT_SR_API_KEY + CONFLUENT_SR_API_SECRET in .env:
//
//   node --env-file=.env scripts/register-schemas.js
//
// Idempotent — re-registering identical content returns the same schema id.

import 'dotenv/config';
import { registerAll, getRegistry, registryReason } from '../src/schemaRegistry.js';

async function main() {
  const reg = getRegistry();
  if (!reg) {
    console.error('FATAL:', registryReason());
    console.error('Fill the three CONFLUENT_SR_* vars in .env first ' +
      '(docs/manager/track-b-notes.md §B2 step 3).');
    process.exit(1);
  }

  console.log('[register-schemas] registering schemas under TopicNameStrategy…');
  const results = await registerAll();

  for (const r of results) {
    console.log(`  ✓ ${r.subject.padEnd(40)}  id=${r.id}  ←  ${r.schemaFile}`);
  }
  console.log(`[register-schemas] done. ${results.length} subject(s) live.`);
}

main().catch(err => {
  console.error('[register-schemas] FAILED:', err?.message || err);
  process.exit(1);
});
