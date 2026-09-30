# Restore the offline Git checkpoint

Use the exact bundle referenced by SHARED_GIT_REMOTE_LATEST.json and verify its SHA256 first. This command explicitly selects the branch advertised by the bundle:

```powershell
git -c core.longpaths=true clone --branch forex <bundle-path> forex
```

The bundle was restored into a fresh folder and all 4,584 files matched the final published GitHub checkout. This corrects the generic command in the earlier immutable final-operation packet; source, bundle bytes, commit and scientific queue are unchanged. Models and large data are retrieved separately through the Vault; cloning does not fit models. For Windows use a short local checkout path and Git long-path support as shown.
