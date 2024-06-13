#!/bin/bash

# Iniciar el producer
echo "Iniciando Producer..."
python Producer.py &

# Esperar un breve momento para asegurarse de que el producer esté corriendo
sleep 5

# Iniciar el consumer
echo "Iniciando Consumer..."
python Consumer.py
