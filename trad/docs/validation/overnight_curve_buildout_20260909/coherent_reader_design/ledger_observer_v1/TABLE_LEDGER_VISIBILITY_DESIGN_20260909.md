# Table-only ledger forecast visibility

The current main HTML (`0af15b1b…`) never reads the new observer namespace. `jointForecastActivity` (lines 210–215) accepts only `data.joint_price_news_forecasts` and normalized `active_forecasts`; `jointForecastCell` (217–223) also interprets missing forecasts as input-readiness failures. The new endpoint instead has an immutable observer report with `summary.rows[].families.ridge_price_news_v1.latest_forecast` and a separate response-time `consumer.active_forecast_ids`. Putting those rows into the old producer object would misstate their provenance.

## Approved minimal change

Add a separate `ledgerForecastActivity` and a table-only selector. Give `availableForecastCoverage` an optional activity argument and pass the selector only from `renderMarketOverview`; its existing calls from the global header and Bot activity keep their existing producer semantics. Preserve the original `jointForecastActivity` and producer Bot activity implementation. No CSS, layout, model, backend, configuration or order change is involved.

The new projection checks the exact API and observer schema, independent-read-only proof basis, original v3 registry, source-closure identity, inert flags, unique 68-pair inventory, original single-family cohort names and consumer ID membership. It checks response/observation/row clocks against actual client time and the original reference → issue → publication → consumption → ledger observation ordering. Each original H1 target must remain ahead of client time. News freshness is checked at original issue time; an original H1 forecast is not erased merely because its news source window has since expired. Invalid or missing rows stay unavailable with the actual ledger reason, never invented warm-up progress.

The browser performs consistency checks against the source-bound server attestation; it does not repeat SQLite validation or claim to independently recompute Python's canonical JSON hashes. Exact report SHA/spec identities bind a cached original report to a newer compact consumer projection. Original decision ID, forecast SHA and cohort must match the active-consumer tuple before a row is shown.

Map the one original arm into a new presentation object only: preserve expected return, probability, reference/issue/target and contribution values; attach original publication/reference-availability/consumption clocks from its containing forecast. Do not mutate or reseal the observer report, rename a cohort, convert the forecast into an order, or fabricate `active_forecasts` in the old producer source.

For ledger-backed cells add the functional source text **“Combined · independently verified from original ledger”**, the original ledger observation time and any original producer transport mismatch/error diagnostic. Keep same-model neutral-news ablation plus news adjustment distinct from the separately fitted price-only comparison. Current market spread and live movement retain their independently checked quote clocks. Missing ledger publication is labelled as such; `current_inputs_not_observed` remains unknown. Producer Bot activity remains separate and may report unavailable transport while the table shows independently verified original forecasts.

## Fetch and concurrency

The existing `refresh` loop (line 359) fetches only `/api/main`. Extend that loop to obtain `/api/joint-v3-ledger-observation` when the compact report SHA changes or no full report is held. Keep at most one full immutable report in memory. A later compact response can reuse it only when the report SHA and API specification SHA match exactly; use that compact response's own consumer ID set and clock without modifying the original full report.

When the full response has advanced to a newer report between requests, accept only its self-contained report/consumer pair with the same API specification and original origin identity and an observation time at least as new as the compact hint. Never combine IDs from one report generation with rows from another. A monotonically increasing refresh request token prevents an older response from repopulating rows after a newer failure or response. An unavailable compact response, HTTP/parse failure, source/spec mismatch, future or stale clock clears ledger display eligibility. Clear the previous report before waiting for a changed report, and render the other market/news/position columns independently. A bounded full-fetch timeout prevents that request from holding the refresh loop indefinitely.

## Focused validation before release

* A real-shaped observer payload with 66 original forecasts and producer generation mismatch produces ledger-labelled table rows while the old producer activity stays unavailable.
* Wrong schema/proof/authority/registry/cohort, duplicate pair or consumer ID, missing receipt flags and mismatched decision/hash/cohort tuples are withheld.
* Stale/future response or ledger clocks and original H1 expiry are withheld; old news expiry after a valid original issue does not erase the H1 forecast.
* Same-hash compact reuse preserves report bytes and original clocks; changed hashes are not mixed. Newer full-generation response is accepted only as a complete report/consumer pair; delayed older responses and fetch failures cannot restore stale rows.
* Missing quote still displays a verified forecast with spread/live movement unavailable. Existing price-only fallback stays explicitly price-only when no verified joint arm exists.
* Snapshot existing HTML and portable-handler-test bytes before editing. Update only the intentionally changed main-HTML baseline in portable tests; embedded HTML, old producer/projector functions and the original producer activity remain unchanged. Use project-contained synthetic JS fixtures, not external draft or live database dependencies.

This design is retained before implementation. It does not claim that the producer publication bug is fixed or that the current forecasts are accurate, profitable or executable.
