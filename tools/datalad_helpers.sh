#!/bin/bash
set -e
CMD=$1
shift
case "$CMD" in
  init)
    datalad create -c text2git datasets
    datalad siblings add -d datasets --name storage --url ria+file:///datahub-storage
    ;;
  clone)
    datalad clone ria+file:///datahub-storage datasets
    ;;
  *)
    echo "Usage: $0 [init|clone]" >&2
    exit 1
    ;;
esac
