# Component extraction reproduction contract

WP6, design14.2–14.4, R05/R14. Reuse the preserved ONS and DOL parsers in an isolated module; never import the collector. Exact frozen inputs and source bytes are pinned. Implicit current-clock access raises. The title adapter maps retained headline to original raw title; the retained first observation is supplied explicitly for diagnostic reproduction, not readiness certification.

Audit all nine components from three source versions. ONS spans use original clean_text coordinates and preserve full normalized text, all matches and native first-match selection. These are not raw HTML byte offsets. Reproduce DOL from the hash-bound archived PDF's frozen pypdf6.15.0 text. PDF capture provenance and raw PDF are included; original HTTP receipt and extraction readiness remain unproven. Pin timezone runtime. No live inputs, fits, surprises, forecasts, historical backdating or source acceptance.

Real parser defects and adversarial cases are evidence for a subsequent separate offline repair. Original source code, outputs and retained numeric state remain preserved. Passing local and relocated tests is separate from independent review. The protected forecast result remains unchanged.
