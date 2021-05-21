
all: clean
	python3 -m build
	python3 -m twine upload --repository aonsq dist/*

clean:
	rm -rf build/ dist/