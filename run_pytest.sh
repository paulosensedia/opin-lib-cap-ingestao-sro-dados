#!/usr/bin/env bash

export PYTHONPATH=./src/

case "$1" in
  --all)
    pytest
    shift
    ;;
  --specific)
    pytest $2
    shift
    ;;
  *) echo "Opcao invalida ou nao informada: $1" ;;
esac