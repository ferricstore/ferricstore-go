#!/usr/bin/env bash
set -euo pipefail

# SDKVersion is release metadata: its value must advance without hiding changes
# to any other exported constant or declaration.
awk '
  /^- SDKVersion: value changed from "[0-9]+\.[0-9]+\.[0-9]+" to "[0-9]+\.[0-9]+\.[0-9]+"$/ { next }
  { print }
'
