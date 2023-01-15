black:
	@python3 -m black .

isort:
	@isort . --tc --up

pylint:
	@pylint .