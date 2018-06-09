all: addPermission execute

addPermission:
	chmod +x run.sh

execute:
	./run.sh