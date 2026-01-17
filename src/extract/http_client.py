"""
Cliente HTTP especializado para la fase de extracción del pipeline ETL.

Contexto:
- Fase: Extracción (Extract)
- Propósito: Realiza peticiones HTTP robustas a la API de datos industriales, con manejo de reintentos y backoff exponencial.
- Dependencias clave: requests

Este módulo abstrae la lógica de comunicación con la API fuente.
"""

import time
import requests
from typing import Any, Dict


class RequestsHttpClient:
    """
    Cliente HTTP con reintentos y backoff para extracción de datos.

    Responsabilidad:
    - Realizar peticiones GET a la API industrial.
    - Implementar lógica de reintentos y manejo de errores transitorios.

    Uso:
    Instanciar con la URL de la API y parámetros opcionales de timeout y reintentos.
    """

    def __init__(
        self,
        url: str,
        timeout: int = 10,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get(self) -> Dict[str, Any]:
        """
        Realiza una petición GET a la API con reintentos y backoff exponencial.

        Intenta obtener la respuesta JSON de la API, reintentando en caso de fallos
        transitorios de red o errores HTTP recuperables.

        Returns:
            dict: Respuesta JSON de la API.

        Raises:
            requests.RequestException: Si se agotan los reintentos sin éxito.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    self.url,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise exc

                sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_time)

        return {}
