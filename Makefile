.PHONY: validate docs

validate:
	pytest -q

docs:
	python scripts/gen_schema_docs.py
	mkdocs build
