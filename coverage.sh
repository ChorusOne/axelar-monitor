#!/bin/sh
set -euo pipefail
cargo tarpaulin -o html --exclude-files 'src/generated/*' --exclude-files 'src/bin/*' --exclude-files 'src/main.rs' --target-dir tarpaulin --skip-clean 
