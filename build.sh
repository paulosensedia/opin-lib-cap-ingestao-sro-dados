#!/usr/bin/env bash

# O script 'build.sh' é utilizado para empacotar o projeto e deverá
# ter o seguinte permissionamento:
#   chmod 744 build.sh

# Para a execução desse script é premissa ter o pacote 'wheel' instalado na máquina.
# Comando de instalação: pip install wheel 

python3 setup.py bdist_wheel