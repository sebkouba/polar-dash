# Repo Agent Notes

- After every change, run the relevant compile or test commands before wrapping up.
- After any change that affects `macos/BreathingBar`, stop the running BreathingBar app before rebuilding.
- After any change that affects `macos/BreathingBar`, replace `/Applications/BreathingBar.app` with the new build.
- After any change that affects `macos/BreathingBar`, launch the installed app from `/Applications`.
- After any change that affects `macos/BreathingBar`, verify that the installed app actually started.
- Prefer `./scripts/deploy-breathingbar-app.sh` for the BreathingBar rebuild, install, sign, and launch flow.
