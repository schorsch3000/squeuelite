.PHONY: lint format lint-php format-php install test autotest

format: lint format-php

lint: lint-php

lint-php:
	find . -not -path "./vendor/*" -name "*.php" -print0 | xargs -0 php -l

format-php:
	find . -not -path "./vendor/*" -name "*.php" -print0 | xargs -0 prettier -w

install: vendor

vendor: composer.json composer.lock
	composer install
	touch vendor

composer.lock: composer.json
	composer install
	touch vendor


test: vendor
	rm -rf testlog
	touch testlog
	./vendor/bin/codecept run || true
	cat testlog
	echo


autotest: vendor
	find | entr -c make test