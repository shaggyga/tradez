# Stage B — isolated legacy pip-unit repair

This directory is an isolated copy of the two legacy rotation consumers and
their reviewed all-68 pip metadata. The live project under `trad` is unchanged.

The repair replaces the old quote-currency shortcut with the hash-bound
all-68 map used by the newer forward updater. It fixes unit handling for
EUR_HUF, HKD_JPY, USD_HUF and USD_THB across indicator/ATR calculations and
rotation synthetic/observed quote cost calculations.

Run only from this directory with the reviewed Python 3.12 environment:

`C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe -I -B run_isolated_pip_tests.py`

The runner loads only this copy's source and writes the machine-readable test
receipt beside it.  It is the command used for the recorded validation.

The tests are focused engineering evidence. They do not run a rotation bot,
read credentials, open the market-data archives, fit a model, replay history,
or authorize a source merge. The next task, after these tests, is to carry this
repair into an isolated all-68 offline replay branch with a new source/data
contract and its own accounting gates.
