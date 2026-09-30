# Review, rollback and final publication

No Git operation is performed by this publisher. The exact pre-documentation source commit remains 2083eb77ebbce103e1a5382969de7810a6e7f962.

If document rollback is required, first reconcile coordination-board ownership and verify every live file still matches FILE_CHANGES.json's after hash. Restore only the affected bytes from before_documents/ and before_project_documents/. A null before hash identifies the new SHARED_GIT.md; preserve/archive it explicitly rather than deleting unrelated work. Restore BEFORE_REVIEW_POINTER.json only after reconciling the current pointer and review queue. Never overwrite newer board/queue/doc edits or delete this evidence.

To finish publication, copy the exact changed project_documents/ bytes into the isolated shared checkout, preserving other files, commit the appendices, and verify/push using ordinary Git operations outside this publisher. Verify numerical source still matches ENGINE_POINTER.json. Then write the mutable Vault SHARED_GIT_REMOTE_LATEST.json with source_commit, handoff_document_commit, remote URL/status, verification evidence hashes, this packet path and MANIFEST.json hash. That operational receipt is not a sealed current document. Never update this package to substitute the later commit for its source anchor.
