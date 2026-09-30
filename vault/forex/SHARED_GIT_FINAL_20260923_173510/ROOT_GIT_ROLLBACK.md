# Unified Git root and rollback

Primary workspace: `C:/Users/zmoor/Documents/forex`, branch `forex`.
The former nested Git metadata is preserved at
`C:/Users/zmoor/Documents/forex/git_publication_20260923/legacy-trad.git`.
It contains the old refs and index; the original 174 commits also remain in the new
repository. `original_status.txt`, `original_index.txt` and `original_refs.txt`
record the pre-migration state. Existing source bytes were not overwritten.

`SOURCE_COMMIT.json` and `clone_verification/RESULTS.json` identify the complete
source checkpoint and fresh bundle-clone verification. New helper/recovered source
files are listed by `ROOT_INSTALL_PREPARED.json`. They remain useful project files,
not regenerated model runs. The final Vault operation receipt identifies the later
documentation commit and replicated bundle.

If metadata rollback is ever necessary, first stop concurrent Git writers and
checkpoint any newer changes. Verify all exact absolute paths remain under this
workspace and that the two intended backup destinations do not already exist.
Move the current root `.git` to a new named backup under this evidence directory;
then move `legacy-trad.git` back to `trad/.git`. Use native PowerShell `Move-Item`
with `-LiteralPath` after those checks. Keep new source, model artifacts, logs and
both Git histories; do not recursively delete files or reset the worktree.

This metadata-only rollback restores the old repository boundary/index. It does
not undo later source edits or remove newer files. Inspect the saved status and
current differences before proceeding. Remote history is preserved; do not force
push or delete a branch to imitate a local rollback.

The GitHub repository's previous `main` branch is retained. The shared Forex
branch is `forex`, now the repository default. The origin URL can be changed later
using `git remote set-url origin <new-url>` without changing run/model identity.
