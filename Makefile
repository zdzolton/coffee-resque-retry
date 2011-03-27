generate-js: deps
	@mkdir -p lib
	@find src -name '*.coffee' |xargs coffee -c -o lib

remove-js:
	@rm -fr lib/

deps:
	@test `which coffee` || echo 'You need to have CoffeeScript in your PATH.\nPlease install it using `npm install coffee-script`.'
	@test `which vows` || echo 'You need to have Vows.js in your PATH.\nPlease install it using `npm install vows`.'

test: deps
	@vows

publish: generate-js
	@test `which npm` || echo 'You need npm to publish'
	npm publish

dev-install: generate-js
	@test `which npm` || echo 'You need npm to do npm link'
	npm link .

.PHONY: all
