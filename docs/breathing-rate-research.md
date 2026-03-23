# Breathing Rate Research Notes

## Summary

The current conclusion is that `ACC-only` breathing-rate estimation is useful but not sufficient. The best path for this project is:

1. Use chest accelerometer motion as one respiration channel.
2. Use ECG-derived respiration as a second channel.
3. Fuse both with a quality gate and temporal continuity constraint.
4. Collect manually labeled breathing phase data to tune and validate the estimator against your own physiology and strap placement.

This is now reflected in the codebase: the estimator fuses ECG- and ACC-derived candidates before persisting `breathing_estimates`.

## What The Literature Suggests

### 1. Tri-axial accelerometers benefit from PCA and autocorrelation

Hostrup et al. (2025) evaluated respiratory-rate estimation from a tri-axial accelerometer and reported strong agreement against a reference method when using:

- low-pass filtering,
- principal component analysis,
- autocorrelation-based respiratory period estimation.

This matters for the H10 because the chest strap gives us a tri-axial accelerometer mounted exactly where respiratory motion is present, but the useful breathing axis is not fixed. PCA is therefore a better default than choosing `x`, `y`, or `z` directly.

Source:
- [Hostrup et al. 2025, PubMed](https://pubmed.ncbi.nlm.nih.gov/40054067/)

### 2. ECG morphology contains respiration information

Respiration modulates ECG through thoracic movement, impedance changes, and electrical-axis changes. Comparative work on ECG-derived respiration (EDR) shows that morphology-based signals can perform well in ambulatory settings, especially features tied to QRS morphology rather than a single naive baseline estimate.

The most relevant implication for this repo is that we should not treat the H10 ECG as useful only for HRV. It also carries respiration information that can stabilize the breathing estimate when accelerometer motion is ambiguous.

Source:
- [Gil et al. 2020, A Comparative Study of ECG-derived Respiration in Ambulatory Monitoring using the Single-lead ECG](https://pmc.ncbi.nlm.nih.gov/articles/PMC7109157/)

### 3. Fusion tends to outperform single-channel RR estimation

Charlton et al. found that top-performing respiratory-rate estimators often fused multiple respiratory modulations rather than relying on a single signal. They also found that temporal smoothing and quality-aware processing improved robustness.

The practical takeaway is simple: for this project, the right default is not “find the strongest peak and trust it,” but rather:

- generate multiple candidate respiration signals,
- score them,
- reject or downweight weak candidates,
- apply temporal continuity.

Source:
- [Charlton et al. 2017, An assessment of algorithms to estimate respiratory rate from the electrocardiogram and photoplethysmogram](https://pmc.ncbi.nlm.nih.gov/articles/PMC5390977/)

## First-Principles Reasoning

Breathing changes more than one physical process at once:

- Chest expansion moves the strap body, which affects accelerometer signals.
- Breathing changes thoracic geometry and impedance, which alters ECG morphology.
- Breathing also modulates autonomic tone, which can leak into RR intervals.

Those signals fail in different ways:

- ACC fails under general body movement.
- ECG morphology fails under electrical noise, poor contact, or morphology distortion.
- RR-interval modulation can be confounded by non-respiratory HRV.

That is why the best estimator is likely a fusion model with explicit confidence, not a single-channel estimator.

## What Was Wrong With The Earlier Estimator

The original estimator in this repo was:

- PCA over tri-axial ACC,
- band-pass filtering,
- Welch peak selection in the respiratory band.

That is cheap and often reasonable, but it has a predictable failure mode: it can jump to a wrong spectral peak when movement, harmonics, or limited window length distort the spectrum.

In practice that showed up as implausible jumps such as `6.7 -> 22.7 -> 12 br/min`.

## Current Direction In The Codebase

The current breathing pipeline now does the following:

- ACC candidate:
  - tri-axial ACC,
  - PCA,
  - low-pass cleanup,
  - band-limited respiration candidate,
  - spectral + autocorrelation agreement score.
- ECG candidate:
  - single-lead ECG,
  - QRS slope-range feature extraction,
  - interpolation to a uniform time series,
  - band-limited respiration candidate,
  - spectral + autocorrelation agreement score.
- Fusion:
  - if ACC and ECG agree, weighted average them,
  - otherwise prefer the higher-quality candidate with a continuity prior,
  - smooth against the previous estimate.

This is still not “ground truth accurate.” It is only more plausible and more stable.

## Why Manual Labeling Matters

The literature gives generic algorithmic guidance, but your breathing, strap position, posture, and movement patterns are specific to you.

If we want to push this toward a research-quality system, we need a labeled dataset with:

- nanosecond timestamps for breathing phase transitions,
- continuous raw ECG and ACC around each label,
- enough repetitions across slow / normal / fast breathing,
- enough posture variation to characterize failure modes.

That lets us tune:

- filter cutoffs,
- window sizes,
- candidate-quality thresholds,
- fusion weights,
- continuity priors,
- phase-aware predictors instead of only scalar RR estimates.

## Recommended Labeling Protocol

The first useful protocol is simple:

- `L`: started inhaling
- `H`: finished inhaling
- `J`: started exhaling
- `K`: finished exhaling

That gives four boundaries per respiratory cycle and is better than a single “breath occurred” event because it lets us:

- estimate inhale duration,
- estimate exhale duration,
- detect inhale/exhale asymmetry,
- supervise phase-aware models,
- build cycle-aligned templates.

## Experiment Design Recommendations

To make the labels useful, run short structured blocks instead of ad-hoc labeling only:

1. Sit still and breathe normally for 3 minutes.
2. Sit still and breathe slowly for 3 minutes.
3. Sit still and breathe faster for 2 minutes.
4. Repeat while standing.
5. Repeat with light natural movement to expose failure cases.

Additional recommendations:

- Keep the labeling window focused so keypress latency stays low.
- Use the same key mapping every time.
- Record a few seconds of quiet before and after each block.
- If possible, occasionally compare against an external reference such as a respiration belt.

## Next Technical Steps

The highest-value next changes after collecting labels are:

1. Persist label events and annotation sessions in SQLite.
2. Export training windows around each labeled event.
3. Add confidence scores to `breathing_estimates`.
4. Train and evaluate a phase-aware model against the labels.
5. Use the labeled data to tune the fusion logic for your own recordings.
