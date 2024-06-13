# Base image with specified Python version
ARG PYTHON_VERSION=3.10.11
FROM python:${PYTHON_VERSION}-slim as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr.
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Create a non-privileged user to run the app.
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --uid "${UID}" \
    appuser

# Actualizamos las setuptools para evitar la incompatibilidad de versiones de dotenv
RUN python -m pip install --upgrade setuptools

# Aseguramos que usa el último pip para instalar los requirements
RUN python -m pip install --upgrade pip

# Copiar el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente y el script de inicio al contenedor
COPY . .

# Copiar el script y cambiar permisos dentro del propio script
COPY start.sh /app/
RUN chmod +x /app/start.sh

# Switch to the non-privileged user to run the application.
USER appuser

# Comando por defecto para ejecutar la aplicación
CMD ["./start.sh"]


