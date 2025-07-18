#!/bin/bash

# Extract package name from package.json
PACKAGE_NAME=$(node -p "require('./package.json').name")

pnpm semantic-release -t "${PACKAGE_NAME}@\${version}" --no-ci
