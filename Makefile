.PHONY: install test prepare-real validate local clean

install:
	python -m pip install -e '.[test]'

test:
	pytest -q

prepare-real:
	python scripts/prepare_real_data.py

validate:
	python scripts/validate_sources.py

local:
	python scripts/run_local.py

clean:
	python -c "import shutil; from pathlib import Path; root=Path('data'); [shutil.rmtree(root/p, ignore_errors=True) for p in ['bronze','silver','gold','observability','quarantine']]"
